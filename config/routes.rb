RsBenchmark::Engine.routes.draw do
  get "/" => "statistics#dayly_volume"
  get "stream_publish_intervals" => "statistics#stream_publish_intervals"
  get "wichtig" => "statistics#wichtig"
  get "benchmark_results" => "statistics#benchmark_results"
end
