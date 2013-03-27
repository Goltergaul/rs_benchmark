# -*- encoding: utf-8 -*-
require File.expand_path('../lib/rs_benchmark/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Dominik Goltermann"]
  gem.email         = ["dominik@goltermann.cc"]
  gem.description   = %q{Tools for benchmarking Recommender Systems}
  gem.summary       = %q{TODO: Write a gem summary}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "rs_benchmark"
  gem.require_paths = ["lib"]
  gem.version       = RsBenchmark::VERSION

  gem.add_dependency 'rubyperf'
  gem.add_dependency 'mongoid'

  gem.add_development_dependency 'debugger'
end
