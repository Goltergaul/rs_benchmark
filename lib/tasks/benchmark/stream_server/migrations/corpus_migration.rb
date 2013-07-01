require "active_record"

ActiveRecord::Base.establish_connection(
  :adapter => "mysql2",
  :database => "james_benchmark_server",
  :user => "root",
  :password => "0815"
)

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