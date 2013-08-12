require "benchmark"

module RsBenchmark

  # This class can be used to measure response times of function calls.
  class ResponseTime

    # This class is used to persist measured response times
    # to MongoDb
    class RsBenchmarkResponseTime
      include Mongoid::Document

      field :tag, :type => String
      field :data, :type => Hash

      index :tag => 1
      index "data.time" => 1

      attr_accessible :tag, :data
    end

    # Measure response time for a given block and save results
    # @param [String] tag Identifier for this kind of measurements. E.g. name of the measured function
    # @param [Block] block The code block to measure
    def self.measure tag, &block
      # If not in benchmark environment simply call block and exit
      if ENV["RAILS_ENV"] != "benchmark"
        block.call
        return
      end

      # Call block while measuring execution time
      bm = Benchmark.measure do
        block.call
      end

      # Persist measured data
      RsBenchmarkResponseTime.create!(
        :tag => tag,
        :data => {
          :time => Time.now,
          :real => bm.real,
          :stime => bm.stime,
          :utime => bm.utime
        }
      )
    end
  end
end