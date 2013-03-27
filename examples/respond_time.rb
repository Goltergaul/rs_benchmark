require "rubygems"
require "bundler/setup"
Bundler.require(:default)

response_time = RsBenchmark::ResponseTime.new(:group_name => "example_tag", :log_folder => File.dirname(__FILE__)+"/logs/")

response_time.benchmark "sleep_operation" do
  sleep 1.25
end

response_time.flush