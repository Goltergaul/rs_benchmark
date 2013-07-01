desc "Create StreamServer corpus table (mysql)"
namespace :rs_benchmark do
  task :migrate => :environment do
    require "active_record"

    throw "Missing config/rs_benchmark.yml" unless RsBenchmark::Engine.benchmark_config

    ActiveRecord::Base.establish_connection(RsBenchmark::Engine.benchmark_config[:mysql])

    class CreateCorpusTable < ActiveRecord::Migration
      def change
        create_table :corpus do |t|
          t.string :title
          t.text :text, :limit => 16777215 #medium text field
          t.integer :length
          t.string :service
        end

        add_index :corpus, :length
        add_index :corpus, :service
      end
    end

    CreateCorpusTable.new.migrate :change
  end
end
