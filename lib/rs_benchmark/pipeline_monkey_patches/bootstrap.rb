if ENV["RAILS_ENV"] == "benchmark"
  puts "Loading monkeypatches for benchmarking /!\\"
  require_relative "response_time_tracking"
  require_relative "twitter" if defined?(Twitter::Client)
  require_relative "fb_graph" if defined?(FbGraph::User)
end