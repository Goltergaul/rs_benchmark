require File.dirname(__FILE__) + "/stream_server.rb"
map "/" do
    run BenchmarkStreamServer::StreamServer
end