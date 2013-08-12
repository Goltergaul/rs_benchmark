namespace :rs_benchmark do
  namespace :workload do

    desc "Runs map reduce tasks => Updates stats"
    task(:reduce_data, [:start, :end] => [:environment]) do |t, args|

      puts "Deleted #{Statistics::Dayly.delete_all} statistic objects"

      ["stream_publish_rate"].each do |task_name|
        puts "Reducing #{task_name}"
        Rake::Task["rs_benchmark:workload:reduce_tasks:#{task_name}"].invoke(args[:start], args[:end])
      end
    end

    namespace :reduce_tasks do

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

    end

  end
end
