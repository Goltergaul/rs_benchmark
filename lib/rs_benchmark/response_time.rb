require "rubyperf"

module RsBenchmark
  class ResponseTime
    def initialize options
      throw ArgumentError.new "Missing :group_name option" unless options[:group_name]
      throw ArgumentError.new "Missing :log_folder option" unless options[:log_folder]
      @options = options
      @results = Hash.new
      @semaphore = Mutex.new

      @rubyperf = Perf::Meter.new
    end

    def benchmark tag, &block
      time_start = Time.now
      @rubyperf.measure tag do
        block.call
      end
      time_end = Time.now
      duration = time_end-time_start

      @semaphore.synchronize {
        @results[tag] = [] unless @results.has_key? tag
        @results[tag] << duration
      }
    end

    def flush
      @semaphore.synchronize {
        @results.each do |tag, values|
          values.each do |value|
            file_handler.puts("#{tag};#{value*1000}")
          end
        end

        @results = {}
      }
    end

    def get_report
      @rubyperf.report_simple
    end

    def file_handler
      return @file_handler if @file_handler

      file_path =  @options[:log_folder]+"/#{@options[:group_name]}/results.csv"
      FileUtils.mkdir_p File.dirname(file_path)
      @file_handler = File.open(file_path,"w")

      @file_handler.puts "Tag;Real Time"

      @file_handler
    end
  end
end