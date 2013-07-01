namespace :benchmark do
  task :generate_workload => :environment do
    begin
      $stdout.reopen("#{Rails.root}/log/stream_server.log", "w")

      # check environment variables
      # example: bind_address=192.168.178.22 ramp_up_step=20 ramp_up_delay=300 sserver_ip=188.194.91.101 scale=0.01 user_count=5 seed=123456
      unless ENV["bind_address"]
        $stderr.puts "You must set the environment variable 'bind_address' to the ip address of this machine"
        next
      end

      unless ENV["sserver_ip"]
        $stderr.puts "You must set the environment variable 'sserver_ip' to your internet ip adress. If you have a router, enable port forwarding to 'bind_address' on port 3333"
        next
      end

      unless ENV["scale"]
        $stderr.puts "You must set the environment variable 'scale'. A value of 1.0 is the normal scale, use a lower value to accelerate the task chains"
        next
      end

      unless ENV["user_count"]
        $stderr.puts "You must set the environment variable 'user_count' to desired number of active users to simulate"
        next
      end

      if ENV["ramp_up_step"] && !ENV["ramp_up_delay"]
        $stderr.puts "You have set the ramp_up_step environment variable. If you do that you must also specify 'ramp_up_delay' in seconds"
        next
      end

      $stderr.puts "The monkeypatches have hardcoded the 'bind_address'. If you are using an other bind_address than 192.168.178.22, please verfiy it is hardcoded correctly."

      require "rs_benchmark/random_generator"
      require "rs_benchmark/response_time"
      require_relative "benchmark/stream_server/stream_server"
      require_relative "benchmark/monkey_patches"

      puts "Using random seed #{BenchmarkStreamServer::SEED} - set environment variable 'seed' to use a specific seed"
      @rand = GSL::Rng.alloc("gsl_rng_mt19937", BenchmarkStreamServer::SEED)
      @probabilities = YAML.load(File.open("#{Rails.root}/workload_specs/spec.yml")).with_indifferent_access
      @user_count = ENV["user_count"].to_i || 10
      @feed_count = (@probabilities["user_feed_ratio"].to_f*@user_count).to_i

      puts "Starting stream simulation server..."
      # start stream server in another thread
      sinatra_thread = Thread.new do
        BenchmarkStreamServer::StreamServer.run! :host => 'localhost', :port => 3333
      end

      # cleanup
      puts "Cleaning up database..."
      User.destroy_all
      GlobalStream.destroy_all
      Delayed::Job.delete_all
      RsBenchmark::Logger::RsBenchmarkLogger.delete_all
      RsBenchmark::ResponseTime::RsBenchmarkResponseTime.delete_all
      Entry.delete_all
      RankedEntry.delete_all
      RedisStore.new.flushall

      SCALE_FACTOR = ENV["scale"].to_f || 1.0
      STREAM_SERVER = "http://192.168.178.22:3333"
      @stream_counter = 0


      # load monkey patches streams
      Rake::Task["benchmark:workload_generators:load_monkey_patches"].invoke
      # create streams
      Rake::Task["benchmark:workload_generators:setup_streams"].invoke
      # setup users
      Rake::Task["benchmark:workload_generators:setup_users"].invoke
      # 2. generate stream schedules
      Rake::Task["benchmark:workload_generators:prepare_login_chains"].invoke

      WorkloadInducer::Inducer.instance.induce!(sinatra_thread)
      sinatra_thread.join
    rescue Exception => e
      puts e.message
      puts e.backtrace
    end
  end

  namespace :workload_generators do

    task :load_monkey_patches do
      puts "Applying monkey patches ... "
      class User::Authentication
        # do not schedule stream after adding it
        def schedule_stream
        end
      end
    end

    task :setup_streams do

      puts "Generating #{@feed_count} potential streams..."
      stream_length_generator = RsBenchmark::PseudoRandomGenerator.new(@probabilities[:streams][:stream_entries_count], BenchmarkStreamServer::SEED)
      @feed_count.times do |i| #FIXME anzahl jenachdem
        url = "#{STREAM_SERVER}/rss/#{@stream_counter}"
        stream = GlobalStream::Rss.new({
          :url => url,
          :title => "Stream Nbr #{@stream_counter}",
          :last_fetched_at => 4.weeks.ago,
        })
        stream.save!(:validate => false)
        @stream_counter += 1

        # save stream config for stream server
        BenchmarkStreamServer::Cache.instance.add_stream :url => url, :length => stream_length_generator.pick, :id => i, :type => :rss
      end
    end

    task :setup_users do
      require "open-uri"
      puts "Creating #{@user_count} users..."

      @uuid = 0
      global_stream_generator = RsBenchmark::UrnRandomGenerator.new(@probabilities[:user][:global_streams_count], BenchmarkStreamServer::SEED)
      private_stream_generator = RsBenchmark::UrnRandomGenerator.new(@probabilities[:user][:private_streams_count], BenchmarkStreamServer::SEED)

      # choose random private stream and make sure not to choose two times the same for the same user
      def create_private_stream user
        type = ""
        if @rand.uniform > 0.5
          type = "User::Authentication::Twitter"
        else
          type = "User::Authentication::Facebook"
        end

        if user.authentications.first && user.authentications.first._type == "User::Authentication::Twitter"
          type = "User::Authentication::Facebook"
        end

        if user.authentications.first && user.authentications.first._type == "User::Authentication::Facebook"
          type = "User::Authentication::Twitter"
        end

        authentications = user.authentications.to_a

        if type == "User::Authentication::Facebook"
          access_token = "access_token_facebook_#{@uuid}"
          authentications << User::Authentication::Facebook.new({
            "uid" => @uuid,
            "access_token" => access_token,
            "fetch" => true,
            "expires_at" => Time.now+ 4.weeks,
            "last_fetched_at" => 4.weeks.ago
          })

          BenchmarkStreamServer::Cache.instance.add_stream :url => "#{STREAM_SERVER}/facebook/#{access_token}", :length => 100, :id => access_token, :type => :facebook
        else
          access_token = "access_token_twitter_#{@uuid}"
          authentications << User::Authentication::Twitter.new({
            "uid" => @uuid,
            "access_token" => access_token,
            "fetch" => true,
            "expires_at" => Time.now+ 4.weeks,
            "last_fetched_at" => 4.weeks.ago
          })

          BenchmarkStreamServer::Cache.instance.add_stream :url => "#{STREAM_SERVER}/twitter/#{access_token}", :length => 100, :id => access_token, :type => :twitter
        end

        user.authentications = authentications
        @stream_counter += 1
        @uuid += 1
      end

      @user_count.times do |i|
        user = User.new(
          :email => "testuser#{i}@test.com",
          :password => "secretsecurepassword353",
          :confirmed_at => Time.now,
          :current_sign_in_at => Time.now,
        )

        # we do not want to create users that have no stream at all
        global_count = private_count = 0
        while global_count == 0 && private_count == 0 do
          global_count = global_stream_generator.pick.to_i
          private_count = private_stream_generator.pick.to_i
        end

        # generate private streams for user
        private_count.times do
          create_private_stream user
        end

        # assign public streams to user
        gs_ids = []
        global_count.times do |gs_count|
          stream_config = {}
          while stream_config[:type] != :rss do
            stream_config = BenchmarkStreamServer::Cache.instance.pick_stream
          end
          gs_ids << GlobalStream::Rss.where(:url => stream_config[:url]).first.id
        end
        user.global_stream_ids = gs_ids
        user.save(:validate => false)

        # puts "created user #{user.id} with #{global_count} global streams and #{private_count} private streams"
      end
    end

    task :prepare_login_chains do
      require_relative "benchmark/workload_inducer/inducer"
      puts "Generating login chains... (Time values will be scaled by #{SCALE_FACTOR})"

      # create generator to pick a user group (first version picks only an user, there are no groups)
      user_groups = @probabilities[:user_reschedule_intervals].keys
      user_group_probabilities = {}
      user_groups.each do |user_id|
        user_group_probabilities[user_id] = 1.0/user_groups.length
      end
      group_name_generator = RsBenchmark::UrnRandomGenerator.new(user_group_probabilities, BenchmarkStreamServer::SEED)

      User.all.each do |user|
        interval_generator = RsBenchmark::UrnRandomGenerator.new(@probabilities[:user_reschedule_intervals][group_name_generator.pick], BenchmarkStreamServer::SEED)

        # def create_task(task_count, max_task_count)
        #   WorkloadInducer::UserScheduleTask.new({:wait_time => interval_generator.pick.seconds * scale_factor}, create_task(task_count+1, max_task_count))
        # end

        task_count = 0
        max_task_count = 20
        first_task_of_chain = task_chain = WorkloadInducer::UserScheduleTask.new({:wait_time => interval_generator.pick.minutes * SCALE_FACTOR, :user_id => user.id})
        while(task_count < max_task_count) do
          task = WorkloadInducer::UserScheduleTask.new({:wait_time => interval_generator.pick.minutes * SCALE_FACTOR, :user_id => user.id})
          task_chain.next_task = task
          task_chain = task
          task_count += 1
        end

        WorkloadInducer::Inducer.instance.add_chain first_task_of_chain
      end
    end
  end
end