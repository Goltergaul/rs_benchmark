require "gsl"

module RsBenchmark
  class Data

    def self.get_intervals_grouped_by_user_id(event)
      grouped_by_user = {}
      Logger::RsBenchmarkLogger.where(:event => event).each do |value|
        grouped_by_user[value.data["user_id"].to_s] = {:times => [], :intervals => []} unless grouped_by_user[value.data["user_id"].to_s]
        grouped_by_user[value.data["user_id"].to_s][:times] << value.data["time"]
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
    end

    def self.get_mean_and_stdev_by_user(event)
      data = get_intervals_grouped_by_user_id(event)
      data.each do |user_id, result|
        next if result[:intervals].count == 0
        intervals = GSL::Vector.alloc(result[:intervals])
        result[:mean] = intervals.mean
        result[:median] = intervals.sort.median_from_sorted_data
        result[:count] = intervals.length
        if result[:count] > 1
          result[:sd] = intervals.sd
          result[:variance] = intervals.variance_m
          result[:cov] = result[:sd] / result[:mean]
        end

        result.delete(:intervals)
        result.delete(:times)
      end

      data
    end

  end
end