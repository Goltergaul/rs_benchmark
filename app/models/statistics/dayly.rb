module Statistics
  class Dayly
    include Mongoid::Document

    field :value, :type => Hash

    index({"value.type" => 1})
    index({"value.service" => 1})

    # builds a histogramm from a query on a collection of entries that look like { my_hash: { some_property: some_value, other_property: some_value}}
    # @param [Mongoid query] query Query to get data
    # @param [String] property Property in the "value" Hash, e.g. "some_property"
    # @param [Integer] bin_count Number of bins the histogram should have
    # @param [String] hash_name The key for the hash that holds the key defined by the param property, e.g. "my_hash"
    # @param [Boolean] logaritmic Whether to scale the bucket sizes of the histogram in logaritmic steps
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


    # build a histogramm from values in an array
    # @param [Array] array The array of values
    # @param [Integer] bin_count The number of bins
    # @param [Boolean] logaritmic Whether to scale the bucket sizes of the histogram in logaritmic steps
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

    # This method is used to extract data from an histogram, calculate relative frequencies and return them as hash. This hash is then merged in the Workload Description File
    # @param [Array,Query] data Array or Query to build a histogramm from
    # @param [String] name Name of the property in the returned hash
    # @param [String] property Name of the property in the hash in the hash of the returned hash, see first lines of function
    # @param [Integer] bin_count Number of bins of the histogram
    # @param [String] hash_name As in the histogram method
    # @param [Boolean] logaritmic Whether to scale the bucket sizes of the histogram in logaritmic steps
    # @param [Boolean] relative_frequencies Whether histogram frequencies should be left as absolute values or not (false = relative frequencies)
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