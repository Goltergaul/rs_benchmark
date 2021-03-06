module RsBenchmark
  class Logger

    # Model for log entries
    class RsBenchmarkLogger
      include Mongoid::Document

      field :event, :type => String
      field :environment, :type => String
      field :data, :type => Hash

      index({:event => 1}, {:background => true})
      index({:environment => 1}, {:background => true})
      index({"data.time" => 1}, {:background => true})
      index "data.service" => 1
      index "data.stream" => 1

      attr_accessible :event, :data, :environment

      def self.write_csv logs, filename
        file_path =  "#{Rails.root}/log/workload/#{Rails.env}/#{filename}.csv"
        FileUtils.mkdir_p File.dirname(file_path)
        file_handler = File.open(file_path,"w")

        logs.each do |log|
          file_handler.write("time_unix;#{log.data["time"].to_i};#{log.data.to_a.join(";")}\n")
        end

        file_handler.close
      end
    end

    def self.get_instance
      @@instance ||= Logger.new
    end

    def self.configure options
      get_instance.set_options options
    end

    # log something tagged by event
    def self.log event, &block
      get_instance.log event, &block
    end

    def initialize
      @data = Hash.new
    end

    def set_options options
      throw "option :activated not set!" unless options.has_key? :activated
      @options = options
    end

    # run the passed block to retrieve the data that should be logged, decorate it with the current time and save it
    def log event, &block
      throw "Logger not configured" unless @options

      return unless @options[:activated]

      data = block.call

      event_data = {
        :time => Time.now
      }.merge data

      RsBenchmarkLogger.create!(:data => event_data, :event => event, :environment => @options[:environment])
    end
  end
end