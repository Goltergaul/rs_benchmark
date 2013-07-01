namespace :benchmark do
  namespace :workload do

    desc "Runs map reduce tasks => Updates stats"
    task(:reduce_data, [:start, :end] => [:environment]) do |t, args|

      puts "deleted #{Statistics::Dayly.delete_all} statistic objects"

      ["user_stats", "rank_worker_stats", "pipeline", "pipeline_overall", "stream_reschedule", "user_reschedules_vs_pipeline",
        "histogramm_stream_lengths", "stream_publish_rate"].each do |task_name|
        puts "reducing #{task_name}"
        Rake::Task["benchmark:workload:reduce_tasks:#{task_name}"].invoke(args[:start], args[:end])
      end
    end

    namespace :reduce_tasks do

      task :user_stats, :start, :end  do |t, args|
        User.all.each do |user|
          Statistics::Dayly.create(:id => "user_stats_#{user.id}", :value => {
            :private_streams_count => user.authentications.count,
            :global_streams_count => user.global_streams.count,
            :type => "user_stats"
          })
        end
      end

      # wahrscheinlich unsinnig
      # task :filter_interval_checker_stats do |t, args|

      #   start_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "interval_checker").asc("data.time").first.data["time"]
      #   end_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "interval_checker").desc("data.time").first.data["time"]
      #   overriden_start_time = Time.parse(args[:start]) rescue start_time
      #   overriden_end_time = Time.parse(args[:end]) rescue end_time

      #   stream_schedules_not_triggered_by_logins = []
      #   RsBenchmark::Logger::RsBenchmarkLogger.between("data.time" => overriden_start_time..overriden_end_time)
      #     .where("event" => "interval_checker").each do |log_entry|
      #       check = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "reschedule_stream_updates").where(:"data.time".gt => log_entry.data["time"]-60).where(:"data.time".lte => log_entry.data["time"])
      #       scheduled_by_logins = []
      #       check.each do |rsu_log|
      #         scheduled_by_logins += User.find(rsu_log.data["user_id"]).global_streams.map(&:id)
      #       end
      #       unless scheduled_by_logins.include? log_entry
      #         entry = Statistics::Dayly.new(:value => log_entry.data)
      #         entry.value["type"] = "filtered_interval_checker_stats"
      #         entry.save!
      #       end
      #   end
      # end

      task :stream_publish_rate, :start, :end do |t, args|

        start_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_existence_checker").asc("data.time").first.data["time"]
        end_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_existence_checker").desc("data.time").first.data["time"]
        overriden_start_time = Time.parse(args[:start]) rescue start_time
        overriden_end_time = Time.parse(args[:end]) rescue end_time

        map = %Q{
          function() {
            emit("stream_publish_rate"+this.data.stream, { service: this.data.service, publish_times: [this.data.time], stream: this.data.stream });
          }
        }

        reduce = %Q{
          function(key, values) {
            var result = { service: "", publish_times: [], stream: "" };
            values.forEach(function(value) {
                value.publish_times.forEach(function(time) {
                  result.publish_times.push(time);
                });
                result.stream = value.stream;
                result.service = value.service;
            });
            return result;
          }
        }

        finalize  = %Q{
          function(key, value) {
            value.type = "stream_publish_rate";
            return value;
          }
        }

        puts "using start time: #{overriden_start_time}"
        puts "using end time: #{overriden_end_time}"
        RsBenchmark::Logger::RsBenchmarkLogger.between("data.time" => overriden_start_time..overriden_end_time).where(:event => "worker_existence_checker").map_reduce(map, reduce).out(:merge => "statistics_daylies").finalize(finalize).first
      end

      task :rank_worker_stats, :start, :end do |t, args|

        start_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank").asc("data.time").first.data["time"]
        end_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank").desc("data.time").first.data["time"]
        overriden_start_time = Time.parse(args[:start]) rescue start_time
        overriden_end_time = Time.parse(args[:end]) rescue end_time

        duration_in_days = (end_time-start_time) / 3600 / 24

        ## rank_worker stats
        map = %Q{
          function() {
            emit("rank_worker_stats"+this.data.time.getUTCHours()+";"+this.data.time.getUTCMinutes(), { body_length: this.data.body_length, article_count: 1 });
          }
        }

        reduce = %Q{
          function(key, values) {
            var result = { body_length: 0, article_count: 0 };
            values.forEach(function(value) {
                result.body_length += value.body_length;
                result.article_count += value.article_count;
            });
            return result;
          }
        }

        finalize  = %Q{
          function(key, value) {
            value.body_length /= #{duration_in_days}; // build average per day
            value.article_count /= #{duration_in_days};
            value.type = "worker_rank";
            return value;
          }
        }

        puts "using start time: #{overriden_start_time}"
        puts "using end time: #{overriden_end_time}"
        RsBenchmark::Logger::RsBenchmarkLogger.between("data.time" => overriden_start_time..overriden_end_time).where(:event => "worker_rank").map_reduce(map, reduce).out(:merge => "statistics_daylies").finalize(finalize).first
      end

      task :pipeline_overall, :start, :end do |t, args|

        start_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank").asc("data.time").first.data["time"]
        end_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank").desc("data.time").first.data["time"]
        overriden_start_time = Time.parse(args[:start]) rescue start_time
        overriden_end_time = Time.parse(args[:end]) rescue end_time

        duration_in_days = (end_time-start_time) / 3600 / 24

        map = %Q{
          function() {
            var time = this.data.time;
            var key = new Date(Date.UTC(time.getUTCFullYear(), time.getUTCMonth(), time.getUTCDate(), time.getUTCHours(), 0, 0));
            if(this.event == "worker_stream_fetcher") {
              emit("pipeline_overall"+key, {
                stream_fetcher_count: 1,
                existence_checker_count: 0,
                fetch_details_count: 0,
                store_db_count: 0,
                rank_count: 0,
                time: key
              });
            } else if(this.event == "worker_existence_checker") {
              emit("pipeline_overall"+key, {
                stream_fetcher_count: 0,
                existence_checker_count: 1,
                fetch_details_count: 0,
                store_db_count: 0,
                rank_count: 0,
                time: key
              });
            } else if(this.event == "worker_fetch_details") {
              emit("pipeline_overall"+key, {
                stream_fetcher_count: 0,
                existence_checker_count: 0,
                fetch_details_count: 1,
                store_db_count: 0,
                rank_count: 0,
                time: key
              });
            } else if(this.event == "worker_store_db") {
              emit("pipeline_overall"+key, {
                stream_fetcher_count: 0,
                existence_checker_count: 0,
                fetch_details_count: 0,
                store_db_count: 1,
                rank_count: 0,
                time: key
              });
            } else if(this.event == "worker_rank") {
              emit("pipeline_overall"+key, {
                stream_fetcher_count: 0,
                existence_checker_count: 0,
                fetch_details_count: 0,
                store_db_count: 0,
                rank_count: 1,
                time: key
              });
            } else {
              throw "error";
            }
          }
        }

        reduce = %Q{
          function(key, values) {
            var result = {
              stream_fetcher_count: 0,
              existence_checker_count: 0,
              fetch_details_count: 0,
              store_db_count: 0,
              rank_count: 0,
              time: 0
            };
            values.forEach(function(value) {
                result.stream_fetcher_count += value.stream_fetcher_count;
                result.existence_checker_count += value.existence_checker_count;
                result.fetch_details_count += value.fetch_details_count;
                result.store_db_count += value.store_db_count;
                result.rank_count += value.rank_count;
                result.time = value.time;
            });
            return result;
          }
        }

        finalize  = %Q{
          function(key, value) {
            value.stream_fetcher_count /= #{duration_in_days}; // build average per day
            value.existence_checker_count /= #{duration_in_days};
            value.fetch_details_count /= #{duration_in_days};
            value.store_db_count /= #{duration_in_days};
            value.rank_count /= #{duration_in_days};
            value.type = "pipeline_overall";
            return value;
          }
        }

        puts "using start time: #{overriden_start_time}"
        puts "using end time: #{overriden_end_time}"
        RsBenchmark::Logger::RsBenchmarkLogger.between("data.time" => overriden_start_time..overriden_end_time).where(:event.in => ["worker_stream_fetcher", "worker_existence_checker", "worker_fetch_details", "worker_store_db", "worker_rank"]).map_reduce(map, reduce).out(:merge => "statistics_daylies").finalize(finalize).first
      end

      task :pipeline, :start, :end do |t, args|
        start_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank").asc("data.time").first.data["time"]
        end_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank").desc("data.time").first.data["time"]
        overriden_start_time = Time.parse(args[:start]) rescue start_time
        overriden_end_time = Time.parse(args[:end]) rescue end_time

        duration_in_days = (end_time-start_time) / 3600 / 24

        map = %Q{
          function() {
            if(this.event == "worker_stream_fetcher") {
              emit("pipeline"+this.data.time.getUTCHours()+";"+this.data.time.getUTCMinutes(), {
                stream_fetcher_count: 1,
                existence_checker_count: 0,
                fetch_details_count: 0,
                store_db_count: 0,
                rank_count: 0
              });
            } else if(this.event == "worker_existence_checker") {
              emit("pipeline"+this.data.time.getUTCHours()+";"+this.data.time.getUTCMinutes(), {
                stream_fetcher_count: 0,
                existence_checker_count: 1,
                fetch_details_count: 0,
                store_db_count: 0,
                rank_count: 0
              });
            } else if(this.event == "worker_fetch_details") {
              emit("pipeline"+this.data.time.getUTCHours()+";"+this.data.time.getUTCMinutes(), {
                stream_fetcher_count: 0,
                existence_checker_count: 0,
                fetch_details_count: 1,
                store_db_count: 0,
                rank_count: 0
              });
            } else if(this.event == "worker_store_db") {
              emit("pipeline"+this.data.time.getUTCHours()+";"+this.data.time.getUTCMinutes(), {
                stream_fetcher_count: 0,
                existence_checker_count: 0,
                fetch_details_count: 0,
                store_db_count: 1,
                rank_count: 0
              });
            } else if(this.event == "worker_rank") {
              emit("pipeline"+this.data.time.getUTCHours()+";"+this.data.time.getUTCMinutes(), {
                stream_fetcher_count: 0,
                existence_checker_count: 0,
                fetch_details_count: 0,
                store_db_count: 0,
                rank_count: 1
              });
            } else {
              throw "error";
            }
          }
        }

        reduce = %Q{
          function(key, values) {
            var result = {
              stream_fetcher_count: 0,
              existence_checker_count: 0,
              fetch_details_count: 0,
              store_db_count: 0,
              rank_count: 0
            };
            values.forEach(function(value) {
                result.stream_fetcher_count += value.stream_fetcher_count;
                result.existence_checker_count += value.existence_checker_count;
                result.fetch_details_count += value.fetch_details_count;
                result.store_db_count += value.store_db_count;
                result.rank_count += value.rank_count;
            });
            return result;
          }
        }

        finalize  = %Q{
          function(key, value) {
            value.stream_fetcher_count /= #{duration_in_days}; // build average per day
            value.existence_checker_count /= #{duration_in_days};
            value.fetch_details_count /= #{duration_in_days};
            value.store_db_count /= #{duration_in_days};
            value.rank_count /= #{duration_in_days};
            value.type = "pipeline";
            return value;
          }
        }

        puts "using start time: #{overriden_start_time}"
        puts "using end time: #{overriden_end_time}"
        RsBenchmark::Logger::RsBenchmarkLogger.between("data.time" => overriden_start_time..overriden_end_time).where(:event.in => ["worker_stream_fetcher", "worker_existence_checker", "worker_fetch_details", "worker_store_db", "worker_rank"]).map_reduce(map, reduce).out(:merge => "statistics_daylies").finalize(finalize).first
      end

      task :stream_reschedule, :start, :end do |t, args|

        start_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event.in => ["reschedule_stream_updates", "interval_checker"]).asc("data.time").first.data["time"]
        end_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event.in => ["reschedule_stream_updates", "interval_checker"]).desc("data.time").first.data["time"]
        overriden_start_time = Time.parse(args[:start]) rescue start_time
        overriden_end_time = Time.parse(args[:end]) rescue end_time

        duration_in_days = (end_time-start_time) / 3600 / 24

        map = %Q{
          function() {
            if(this.event == "interval_checker") {
              var stream = this.data.stream;
              if(!isNaN(parseFloat(stream)) && isFinite(stream)) {
                // its a private stream, because its a number!
                emit("stream_reschedule"+this.data.time.getUTCHours()+";"+this.data.time.getUTCMinutes(), { global_streams_count:0, private_streams_count: 1, schedule_count: 1 });
              } else {
                emit("stream_reschedule"+this.data.time.getUTCHours()+";"+this.data.time.getUTCMinutes(), { global_streams_count:1, private_streams_count: 0, schedule_count: 1 });
              }
            } else {
              emit("stream_reschedule"+this.data.time.getUTCHours()+";"+this.data.time.getUTCMinutes(), { global_streams_count: this.data.global_streams_count, private_streams_count: this.data.private_streams_count, schedule_count: 1 });
            }
          }
        }

        reduce = %Q{
          function(key, values) {
            var result = { uids: [], global_streams_count: 0, private_streams_count: 0, schedule_count: 0 };
            values.forEach(function(value) {
                result.global_streams_count += value.global_streams_count;
                result.private_streams_count += value.private_streams_count;
                result.schedule_count += value.schedule_count;
            });
            return result;
          }
        }

        finalize  = %Q{
          function(key, value) {
            value.global_streams_count /= #{duration_in_days}; // build average per day
            value.private_streams_count /= #{duration_in_days};
            value.schedule_count /= #{duration_in_days};
            value.type = "reschedule_stream_updates";
            return value;
          }
        }

        puts "using start time: #{overriden_start_time}"
        puts "using end time: #{overriden_end_time}"
        RsBenchmark::Logger::RsBenchmarkLogger.between("data.time" => overriden_start_time..overriden_end_time).where(:event.in => ["reschedule_stream_updates", "interval_checker"]).map_reduce(map, reduce).out(:merge => "statistics_daylies").finalize(finalize).first
      end

      task :user_reschedules_vs_pipeline, :start, :end do |t, args|
        map = %Q{
          function() {
            var time = this.data.time;
            var key = new Date(Date.UTC(time.getUTCFullYear(), time.getUTCMonth(), time.getUTCDate(), time.getUTCHours(), 0, 0));
            if(this.event == "reschedule_stream_updates") {
              emit(key, { uids: [this.data.user_id], streams: this.data.global_streams_count+this.data.private_streams_count, article_count: 0, abort_rate: 0, shortcut_rate: 0, interval_streams: 0 });
            } else if(this.event == "worker_rank") {
              emit(key, { uids: [], streams: 0, article_count: 1, abort_rate: 0, shortcut_rate: 0, interval_streams: 0 });
            } else if(this.event == "worker_existence_checker") {
              var abort_rate = this.data.already_in_progress ? 1 : 0;
              var shortcut_rate = this.data.already_in_db ? 1 : 0;
              emit(key, { uids: [], streams: 0, article_count: 0, abort_rate: abort_rate, shortcut_rate: shortcut_rate, interval_streams: 0 })
            } else {
              emit(key, { uids: [], streams: 0, article_count: 0, abort_rate: 0, shortcut_rate: 0, interval_streams: 1 });
            }
          }
        }

        reduce = %Q{
          function(key, values) {
            var result = { uids: [], streams: 0, article_count: 0, abort_rate: 0, shortcut_rate: 0, interval_streams: 0 };
            values.forEach(function(value) {
                result.article_count += value.article_count;
                result.streams += value.streams;
                result.abort_rate += value.abort_rate;
                result.shortcut_rate += value.shortcut_rate;
                result.interval_streams += value.interval_streams;
                value.uids.forEach(function(uid) {
                  result.uids.push(uid);
                });
            });
            return result;
          }
        }

        finalize  = %Q{
          function(key, value) {
            value.type = "user_vs_rank_worker";
            return value;
          }
        }
        start_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event.in => ["reschedule_stream_updates", "worker_rank", "worker_existence_checker", "interval_checker"]).asc("data.time").first.data["time"]
        end_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event.in => ["reschedule_stream_updates", "worker_rank", "worker_existence_checker", "interval_checker"]).desc("data.time").first.data["time"]
        overriden_start_time = Time.parse(args[:start]) rescue start_time
        overriden_end_time = Time.parse(args[:end]) rescue end_time
        puts "using start time: #{overriden_start_time}"
        puts "using end time: #{overriden_end_time}"
        RsBenchmark::Logger::RsBenchmarkLogger.between("data.time" => overriden_start_time..overriden_end_time).where(:event.in => ["reschedule_stream_updates", "worker_rank", "worker_existence_checker", "interval_checker"]).map_reduce(map, reduce).out(:merge => "statistics_daylies").finalize(finalize).first
      end

      task :histogramm_stream_lengths, :start, :end do |t, args|
        map = %Q{
          function() {
            if(this.event == "worker_stream_fetcher") {
              emit("histogramm_stream_lengths"+this.data.stream, { stream_count: 1, stream_length: this.data.stream_entries_count, users_subscribed_count: 0 });
            } else {
              emit("histogramm_stream_lengths"+this.data.stream, { stream_count: 0, stream_length: 0, users_subscribed_count: this.data.users_subscribed_count });
            }
          }
        }

        reduce = %Q{
          function(key, values) {
            var result = { stream_count: 0, stream_length: 0, users_subscribed_count: 0 };
            values.forEach(function(value) {

              // zaehle mit wie oft der stream geladen wurde um eine durchschnittliche laenge bilden zu koennen
              result.stream_count += value.stream_count;
              result.stream_length += value.stream_length;

              // nimm den groessten count, das entspricht der neuesten usage-statistik bzw dem worst_case (evt lieber durchschnitt?)
              if(value.users_subscribed_count > result.users_subscribed_count) {
                result.users_subscribed_count = value.users_subscribed_count;
              }
            });

            result.avg_stream_length = result.stream_length/result.stream_count;

            return result;
          }
        }

        finalize  = %Q{
          function(key, value) {
            value.type = "stream_lengths_user_count";
            if(value.avg_stream_length) {
              return value;
            }
          }
        }

        start_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event.in => ["worker_stream_fetcher", "worker_store_db"]).asc("data.time").first.data["time"]
        end_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event.in => ["worker_stream_fetcher", "worker_store_db"]).desc("data.time").first.data["time"]
        overriden_start_time = Time.parse(args[:start]) rescue start_time
        overriden_end_time = Time.parse(args[:end]) rescue end_time
        puts "using start time: #{overriden_start_time}"
        puts "using end time: #{overriden_end_time}"
        RsBenchmark::Logger::RsBenchmarkLogger.between("data.time" => overriden_start_time..overriden_end_time).where(:event.in => ["worker_stream_fetcher", "worker_store_db"]).map_reduce(map, reduce).out(:merge => "statistics_daylies").finalize(finalize).first
      end
    end

  end
end
