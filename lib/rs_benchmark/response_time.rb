require "rubyperf"
require "benchmark"

module RsBenchmark
  class ResponseTime

    class RsBenchmarkResponseTime
      include Mongoid::Document

      field :tag, :type => String
      field :data, :type => Hash

      index :tag => 1
      index "data.time" => 1

      attr_accessible :tag, :data
    end

    def self.benchmark tag, &block
      # if not in benchmark environment simply call block and exit
      if ENV["RAILS_ENV"] != "benchmark"
        block.call
        return
      end

      bm = Benchmark.measure do
        block.call
      end

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