namespace :rs_benchmark do
  task :generate_specs => :environment do
    @yaml_hash = {}

    tw_count = User.elem_match(:authentications => {:_type => "User::Authentication::Twitter"}).where(:authentications.with_size => 1).count
    fb_count = User.elem_match(:authentications => {:_type => "User::Authentication::Facebook"}).where(:authentications.with_size => 1).count
    fb_probabillity = fb_count/(tw_count+fb_count).to_f
    @yaml_hash.deep_merge!("facebook_twitter_stream_probabillity" => fb_probabillity)

    Rake::Task["rs_benchmark:specification_generators:article_lengths"].invoke
    Rake::Task["rs_benchmark:specification_generators:rss_feed_lengths"].invoke
    Rake::Task["rs_benchmark:specification_generators:user_vote_intervals"].invoke
    Rake::Task["rs_benchmark:specification_generators:user_profile_size"].invoke
    Rake::Task["rs_benchmark:specification_generators:user_reschedules_intervals"].invoke
    Rake::Task["rs_benchmark:specification_generators:user_streams"].invoke
    Rake::Task["rs_benchmark:specification_generators:facebook_post_types"].invoke
    Rake::Task["rs_benchmark:specification_generators:stream_publish_rate"].invoke

    # clear profile entries that do not have all properties
    @yaml_hash["user_profile"].each do |user_id, profile|
      @yaml_hash["user_profile"].delete(user_id) if !profile.has_key?("word_count") ||
        !profile.has_key?("rated_entries") || !profile.has_key?("reschedule_intervals") ||
        !profile.has_key?("global_stream_count") || !profile.has_key?("private_streams_count")
    end

    Rake::Task["rs_benchmark:specification_generators:rss_user_count"].invoke

    # Calculate ratio of rss-feeds/users (only active users and their feeds)
    users = @yaml_hash["user_profile"].map do |user_id, profile|
      User.find(user_id)
    end
    global_streams = users.map do |u|
      u.global_streams
    end.flatten.uniq
    @yaml_hash.deep_merge!("user_feed_ratio" => global_streams.count/users.count.to_f)

    # Save specification as yaml file
    FileUtils.mkdir_p "#{Rails.root}/workload_specs/"
    File.open("#{Rails.root}/workload_specs/spec.yml", 'w') do |f|
      f.write(@yaml_hash.to_yaml)
    end
  end

  namespace :specification_generators do

    task :article_lengths do
      ["rss", "facebook", "twitter"].each do |service|
        body_lengths = Entry.where(:service => service).map do |entry|
          entry.body.nil? ? 0 : entry.body.length
        end
        @yaml_hash.deep_merge!(Statistics::Dayly.extract_workload_spec(body_lengths, "#{service}_properties", "body_length", 300, "value", (service == "twitter" ? false : true)))
      end
    end

    task :rss_feed_lengths do
      # get average stream length of every stream
      gs_data = GlobalStream::Rss.all.map do |gs|
        lengths = RsBenchmark::Logger::RsBenchmarkLogger.where("event" => "worker_stream_fetcher", "data.stream" => gs.url).map do |le|
          le.data["stream_entries_count"]
        end
        # build average
        if lengths.count == 0
          next
        else
          lengths.sum/lengths.count.to_f
        end
      end
      gs_data.delete(0.0) # feeds with zero length are not valid, remove from histogram
      @yaml_hash.deep_merge!(Statistics::Dayly.extract_workload_spec(gs_data, "streams", "stream_entries_count", 20))
    end

    task :rss_user_count do
      users = @yaml_hash["user_profile"].map do |user_id, profile|
        User.find(user_id)
      end
      global_streams = users.map do |u|
        u.global_streams
      end.flatten.uniq

      gs_stats = {}
      sum = 0
      # calculate abs frequencies
      global_streams.each do |s|
        gs_stats[s.user_ids.count] = 0 unless gs_stats[s.user_ids.count]
        gs_stats[s.user_ids.count] += 1
        sum += 1
      end

      @yaml_hash.deep_merge!({
        "rss_user_count" => gs_stats
      })
    end

    task :facebook_post_types do

      status_count = Entry::Facebook::Status.count
      checkin_count = Entry::Facebook::Checkin.count
      link_count = Entry::Facebook::Link.count
      photo_count = Entry::Facebook::Photo.count
      video_count = Entry::Facebook::Video.count
      overall_count = status_count+checkin_count+link_count+photo_count+video_count

      @yaml_hash.deep_merge!({
        "facebook_properties" => {
          "status_types" => {
            "status" => status_count,
            "checkin" => checkin_count,
            "link" => link_count,
            "photo" => photo_count,
            "video" => video_count
          }
        }
      })
    end

    task :user_streams do
      User.all.each do |user|
        @yaml_hash.deep_merge!({"user_profile" => {user.id.to_s => {"global_stream_count" => user.global_streams.count, "private_streams_count" => user.authentications.count}}})
      end
    end

    task :user_reschedules_intervals do
      require "rs_benchmark/data"
      grouped_by_user = RsBenchmark::Data.get_intervals_grouped_by_user_id("reschedule_stream_updates")
      grouped_by_user.each do |user_id, data|
        user = User.where(:id => user_id).first
        next unless user
        next if user.current_sign_in_at - user.created_at < 2.weeks
        next if data[:intervals].count < 10 # use only users that logged in at least 20 times for more representative data

        hash = Statistics::Dayly.extract_workload_spec data[:intervals], "user_reschedule_intervals", user_id, 300, "", true

        hash["user_reschedule_intervals"].each do |user_id, intervals|
          @yaml_hash.deep_merge!({"user_profile" => {user_id.to_s => {"reschedule_intervals" => intervals}}})
        end
      end
    end

    task :stream_publish_rate do

      ["rss", "facebook", "twitter"].each do |service|
        publish_intervals = []
        Statistics::Dayly.where("value.type" => "stream_publish_rate", "value.service" => service).each do |stream|
          times = stream["value"]["publish_times"].sort
          times.each_with_index do |time, index|
            if index >= 1
              value1 = times[index-1]
              value2 = time
              publish_intervals << (value2-value1)
            end
          end
        end
        publish_intervals.delete(nil)
        publish_intervals.delete(0.0)

        @yaml_hash.deep_merge!(Statistics::Dayly.extract_workload_spec(publish_intervals, "publish_intervals", service, 45, nil, true))
      end
    end

    task :user_vote_intervals do
      grouped_by_user = {}
      Rating.all.each do |rating|
        grouped_by_user[rating.user_id.to_s] = {:times => [], :intervals => []} unless grouped_by_user[rating.user_id.to_s]
        grouped_by_user[rating.user_id.to_s][:times] << rating.created_at
      end

      grouped_by_user.each do |user_id, data|
        times = data[:times].sort
        times.each_with_index do |time, index|
          if index >= 1
            value1 = times[index-1]
            value2 = time
            grouped_by_user[user_id][:intervals] << (value2-value1)/60.0
          end
        end
        data[:intervals].delete(nil)
      end
      grouped_by_user

      mean_of_means_sum = 0
      mean_of_means_count = 0
      grouped_by_user.each do |user_id, data|
        next if data[:intervals].count < 25
        user = User.find(user_id)
        next if user.current_sign_in_at - user.created_at < 2.weeks
        mean = data[:intervals].sum/data[:intervals].count.to_f
        mean_of_means_sum += mean
        mean_of_means_count += 1
      end
      @yaml_hash.deep_merge!({"user_voting" => { "mean" => mean_of_means_sum/mean_of_means_count.to_f}})
    end

    task :user_profile_size do
      User.all.each do |user|
        next if user.current_sign_in_at - user.created_at < 2.weeks
        @yaml_hash.deep_merge!({"user_profile" => { user.id.to_s => {
          "word_count" => user.attribute_ratings.where(:kind => "word").count,
          "rated_entries" => Rating.where(:user_id => user.id).count
        }}})
      end
    end
  end

  task :generate_corpus => :environment do
    require "mysql2"
    require "active_record"
    ActiveRecord::Base.establish_connection(RsBenchmark::Engine.benchmark_config[:mysql])

    class Corpus < ActiveRecord::Base
      attr_accessible :length, :text, :service, :title
    end

    Corpus.connection.execute("ALTER TABLE corpus AUTO_INCREMENT = 1")
    Corpus.connection.execute("TRUNCATE TABLE corpus")

    Entry.all.each do |entry|
      next if entry.body.nil?
      Corpus.create!(
        :length => entry.body.length,
        :text => entry.body,
        :service => entry.service,
        :title => entry.respond_to?(:title) ? entry.title : ""
      )
    end
  end
end