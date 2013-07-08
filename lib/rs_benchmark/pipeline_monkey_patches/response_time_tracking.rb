Workers::BasicWorker.class_eval do
  def after_fail_hook payload

    # log failure for stats
    RsBenchmark::ResponseTime::RsBenchmarkResponseTime.create!(
      :tag => "failure",
      :data => {
        :time => Time.now,
        :worker => self.class.name
      }
    )

    Workers::Logger.info("TRACKING "+self.class.name, "fail!")
    @redis.hdel "response_time_tracking_#{payload.stream}_articles", payload.id
    unless @redis.exists "response_time_tracking_#{payload.stream}_articles"
      Workers::Logger.info("TRACKING "+self.class.name, "fail, deleting everything!")
      @redis.del "response_time_tracking_#{payload.stream}_first_arrived"
      @redis.del "response_time_tracking_#{payload.stream}"
    end
  end

  def after_consume_hook payload
    log_success
  end

  # log success (for throughput)
  def log_success
    RsBenchmark::ResponseTime::RsBenchmarkResponseTime.create!(
      :tag => "success",
      :data => {
        :time => Time.now,
        :worker => self.class.name
      }
    )
  end
end

if defined?(Workers::ExistenceChecker)
  Workers::ExistenceChecker.class_eval do
    def after_consume_hook payload
      log_success
      @redis.hset "response_time_tracking_#{payload.stream}_articles", payload.id, Time.now.to_i
    end
  end
end

if defined?(Workers::Rank)
  Workers::Rank.class_eval do
    def after_consume_hook payload
      log_success

      if !@redis.exists("response_time_tracking_#{payload.stream}_first_arrived") && @redis.exists("response_time_tracking_#{payload.stream}")
        @redis.set("response_time_tracking_#{payload.stream}_first_arrived", true, false)

        time_start = Time.at(@redis.get("response_time_tracking_#{payload.stream}").to_i)
        duration = Time.now - time_start

        RsBenchmark::ResponseTime::RsBenchmarkResponseTime.create!(
          :tag => "response_time_stream_first",
          :data => {
            :time => Time.now,
            :real => duration.to_i,
            :stime => nil,
            :utime => nil
          }
        )
      end

      @redis.hdel "response_time_tracking_#{payload.stream}_articles", payload.id

      if !@redis.exists("response_time_tracking_#{payload.stream}_articles") && @redis.exists("response_time_tracking_#{payload.stream}")
        # all articles of the stream have been processed
        time_start = Time.at(@redis.get("response_time_tracking_#{payload.stream}").to_i)
        duration = Time.now - time_start

        RsBenchmark::ResponseTime::RsBenchmarkResponseTime.create!(
          :tag => "response_time_stream_all",
          :data => {
            :time => Time.now,
            :real => duration.to_i,
            :stime => nil,
            :utime => nil
          }
        )

        # cleanup
        @redis.del "response_time_tracking_#{payload.stream}_first_arrived"
        @redis.del "response_time_tracking_#{payload.stream}"
      end
    end
  end
end

if defined?(Workers::StreamFetcher)
  Workers::StreamFetcher.class_eval do
    def before_consume_hook payload
      unless @redis.exists "response_time_tracking_#{payload.stream}"
        @redis.set "response_time_tracking_#{payload.stream}", Time.now.to_i, false
      end
    end

    def after_fail_hook payload
      @redis.del "response_time_tracking_#{payload.stream}"
    end
  end
end