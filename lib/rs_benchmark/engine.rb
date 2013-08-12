require "rs_benchmark/logger"
require "rs_benchmark/data"
require "rs_benchmark/response_time"

module RsBenchmark
  # An engine is used to plug this gem into the hosting rails app and provide own routes, controllers and models
  class Engine < ::Rails::Engine
    isolate_namespace RsBenchmark

    mattr_accessor :benchmark_config

    initializer "load_configuration" do |app|
      fpath = Rails.root.join('config', "rs_benchmark.yml")
      if File.exists?(fpath)
        self.benchmark_config = YAML.load_file(fpath).with_indifferent_access
      end
    end
  end
end
