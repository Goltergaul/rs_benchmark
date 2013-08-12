namespace :rs_benchmark do

  # starts the stream simulation server for demo purposes with 3 different streams
  task :demo_stream_server => :environment do
    require_relative "benchmark/stream_server/stream_server"

    BenchmarkStreamServer::Cache.instance.add_stream :url => "http://localhost:3333/streams/0", :length => 10, :id => 0, :type => :rss
    BenchmarkStreamServer::Cache.instance.add_stream :url => "http://localhost:3333/facebook/1", :length => 100, :id => "1", :type => :facebook
    BenchmarkStreamServer::Cache.instance.add_stream :url => "http://localhost:3333/twitter/2/0", :length => 200, :id => "2", :type => :twitter

    BenchmarkStreamServer::StreamServer.run! :port => 3334
  end
end