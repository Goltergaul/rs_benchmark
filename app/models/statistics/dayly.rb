module Statistics
  class Dayly
    include Mongoid::Document

    field :value, :type => Hash

    index({"value.type" => 1})
    index({"value.service" => 1})

    def self.group_by_minute_and_hour prefix, query
      query = query.to_a

      result = {
        :hourly => [],
        :minutely => []
      }

      hour_sums = {}
      (0..23).each do |hour|
        (0..59).each do |minute|
          if minute == 30
            result[:hourly] << hour_sums
            hour_sums = {}
          end

          minute_data = query.find do |result|
            result["_id"] == "#{prefix}#{hour};#{minute}"
          end

          if minute_data
            result[:minutely] << minute_data["value"]
            minute_data["value"].each do |key,value|
              if value.instance_of?(Float) || value.instance_of?(Fixnum)
                hour_sums[key] = 0 unless hour_sums.has_key?(key)
                hour_sums[key] += value
              end
            end
          else
            result[:minutely] << {}
          end
        end
      end
      result[:hourly] << hour_sums

      return result
    end

    def self.histogramm query, property, bin_count = 20, hash_name = "value", logaritmic = false
      maximum = query.max("#{hash_name}.#{property}")
      minimum = query.min("#{hash_name}.#{property}")
      puts "warning: minimum is nil, property #{property}, hash_name #{hash_name}" unless minimum
      puts "warning: maximum is nil, property #{property}, hash_name #{hash_name}" unless maximum
      number_of_bins = ((maximum - minimum) / bin_count).ceil
      a = 10**bin_count / maximum

      map = %Q{
        function() {
          var value = this.#{hash_name}.#{property};
          var stepSize = #{maximum}/#{bin_count};
          var bin;

          if(#{logaritmic}) {
            bin = Math.ceil((Math.log(value*#{a}) / Math.LN10)*6);
          } else {
            bin = Math.ceil(value / stepSize);
          }

          emit(bin, { frequency: 1, bin_start: Math.floor(bin*stepSize,0), sum: value });
        }
      }

      reduce = %Q{
        function(key, values) {
          var result = { frequency: 0, sum: 0, real_value: 0, real_values: [] };
          values.forEach(function(value) {
              result.frequency += value.frequency;
              result.sum += value.sum;

              if(#{logaritmic}) {
                result.bin_start = Math.floor(Math.pow(10,(key/6.0))/#{a});
              } else {
                result.bin_start = value.bin_start;
              }
          });
          return result;
        }
      }

      finalize  = %Q{
        function(key, value) {
          if(value.frequency === 0) {
            value.avg = 0.0;
          } else {
            value.avg = value.sum / value.frequency;
          }

          return value;
        }
      }

      result = query.map_reduce(map, reduce).out(:inline => 1).finalize(finalize).to_a

      # fill holes with no entries
      if logaritmic == false
        bin_count.times do |i|
          bin_start = (i*maximum/bin_count.to_f).floor
          unless result.index { |x| x["value"]["bin_start"] == bin_start }
            result << {
              "value" => {
                "frequency" => 0,
                "bin_start" => bin_start,
                "avg" => bin_start.to_f
              }
            }
          end
        end
      end

      result.sort! do |a,b|
        a["value"]["bin_start"] <=> b["value"]["bin_start"]
      end
      frequencies = result.map do |result|
        result["value"]["frequency"]
      end
      frequencies_labels = result.map do |result|
        result["value"]["bin_start"]
      end
      avg_labels = result.map do |result|
        result["value"]["avg"]
      end

      {
        :data => frequencies,
        :labels => frequencies_labels,
        :avg_labels => avg_labels
      }
    end

    # exponential bin size: z = maximum / sqrt(bin_count) => (value/z)^2 => bin

    def self.histogramm_from_array array, bin_count = 20, logaritmic = false
      array.delete(nil)
      array.sort!
      maximum = array.max
      minimum = array.min
      step_size = ((maximum - minimum) / bin_count.to_f).ceil
      a = 10**bin_count / maximum

      result = {}
      sums = {}
      array.each do |value|
        bin = 0
        label = 0
        if logaritmic
          bin = (value == 0.0) ? 0 : (Math.log10(value*a)*6).ceil
        else
          bin = (value/step_size.to_f).ceil
        end
        result[bin] ||= 0
        result[bin] += 1
        sums[bin] ||= 0
        sums[bin] += value
      end

      # fill holes with no entries
      if logaritmic == false
        bin_count.times do |i|
          unless result[i]
            result[i] = 0
            sums[i] = 0
          end
        end
      end

      result_hash = {
        :data => [],
        :labels => [],
        :avg_labels => []
      }

      result.keys.sort.each do |bin|
        result_hash[:data] << result[bin]
        if logaritmic
          result_hash[:labels] << ((10**(bin/6.0))/a).floor
        else
          result_hash[:labels] << bin*step_size
        end
        avg = (result[bin] == 0) ? 0.0 : sums[bin]/result[bin].to_f
        result_hash[:avg_labels] << avg
      end

      result_hash
    end

    def self.extract_workload_spec data, name, property, bin_count=20, hash_name="value", logaritmic = false, relative_frequencies=true
      yaml_hash = {
        name => {
          property => {}
        }
      }
      result_hash = yaml_hash[name][property]

      if data.is_a? Array
        histogramm = histogramm_from_array data, bin_count, logaritmic
      else
        histogramm = self.histogramm data, property, bin_count, hash_name, logaritmic
      end

      total_frequency_count = histogramm[:data].sum
      histogramm[:data].each_with_index do |frequency, i|
        next if frequency == 0.0
        value = histogramm[:avg_labels][i] # use average of bin values as value instead of upper bin size limit
        if relative_frequencies
          probabillity = frequency/total_frequency_count.to_f
          result_hash[value] = probabillity
        else
          result_hash[value] = frequency.to_i
        end
      end

      yaml_hash
    end
  end
end