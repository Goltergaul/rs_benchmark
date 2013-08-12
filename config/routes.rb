RsBenchmark::Engine.routes.draw do
  get "/" => "statistics#index"
  get "stream_publish_intervals" => "statistics#stream_publish_intervals"
  get "benchmark_results" => "statistics#benchmark_results"
end
