require "rubygems"
require "bundler/setup"
require "sinatra/base"
require "debugger"
require "singleton"
require "mongoid"
require "rs_benchmark/random_generator"
require "active_record"

throw "Missing config/rs_benchmark.yml" unless RsBenchmark::Engine.benchmark_config
ActiveRecord::Base.establish_connection(RsBenchmark::Engine.benchmark_config[:mysql])

SCALE_FACTOR ||= 0.0001

module BenchmarkStreamServer

  BenchmarkStreamServer::SEED = ENV["seed"] ? ENV["seed"].to_i : rand(1000000).to_i

  class Corpus < ActiveRecord::Base
    attr_accessible :length, :text, :service, :title
  end

  class Cache
    include Singleton

    def initialize
      puts "Creating new stream cache"
      @streams = {}
    end

    def add_stream stream
      stream[:articles] = [] unless stream[:articles]
      @streams[stream[:id]] = stream

      self
    end

    def get_stream id
      @streams[id]
    end

    # return a random stream
    def pick_stream
      @streams[@streams.keys[rand(@streams.keys.length-1)]]
    end
  end

  class StreamServer < Sinatra::Base
    set :server, 'webrick'
    set :bind, ENV["bind_address"]
    set :views, File.join(File.dirname(__FILE__), *%w[views])
    set :environment, :development
    use ActiveRecord::ConnectionAdapters::ConnectionManagement

    configure do
      root = defined?(Rails)? Rails.root : ENV["project_root"]
      set :probabilities, YAML.load(File.open("#{root}/workload_specs/spec.yml")).with_indifferent_access
      set :global_article_counter, 0

      set :rss_publish_interval_generator, RsBenchmark::PseudoRandomGenerator.new(settings.probabilities[:publish_intervals][:rss], BenchmarkStreamServer::SEED)
      set :rss_body_length_generator, RsBenchmark::PseudoRandomGenerator.new(settings.probabilities[:rss_properties][:body_length], BenchmarkStreamServer::SEED)

      set :facebook_publish_interval_generator, RsBenchmark::PseudoRandomGenerator.new(settings.probabilities[:publish_intervals][:facebook], BenchmarkStreamServer::SEED)
      set :facebook_body_length_generator, RsBenchmark::PseudoRandomGenerator.new(settings.probabilities[:facebook_properties][:body_length], BenchmarkStreamServer::SEED)
      set :facebook_type_generator, RsBenchmark::UrnRandomGenerator.new(settings.probabilities[:facebook_properties][:status_types], BenchmarkStreamServer::SEED)

      set :twitter_publish_interval_generator, RsBenchmark::PseudoRandomGenerator.new(settings.probabilities[:publish_intervals][:twitter], BenchmarkStreamServer::SEED)
      set :twitter_body_length_generator, RsBenchmark::PseudoRandomGenerator.new(settings.probabilities[:twitter_properties][:body_length], BenchmarkStreamServer::SEED)

      set :cache, {}
    end

    def get_corpus length, service
      service = service.to_s
      length = length.to_i

      count = 0
      if settings.cache[:"sql_count_#{length}_#{service}"]
        count = settings.cache[:"sql_count_#{length}_#{service}"]
      else
        count = Corpus.where("corpus.length >= ? AND corpus.service = ?", length, service).count
        settings.cache[:"sql_count_#{length}_#{service}"] = count
      end

      corpus = Corpus.where("corpus.length >= ? AND corpus.service = ?", length, service).limit(1).offset(rand(count)).first
      throw "no corpus of length #{length} available! corpus count #{Corpus.count}" unless corpus
      body = corpus.text[0...length]
      throw "corpus not long enough is: #{body.length}-#{corpus.text.length}-#{corpus.length}, should: #{length}" if body.length < length

      corpus
    end

    def generate_rss_article length, time, stream_id
      corpus = get_corpus(length, :rss)
      title = corpus.title
      title = corpus.text[0..20] if title.blank?
      settings.global_article_counter += 1

      {
        :title => title,
        :pub_date => time,
        :body => corpus.text,
        :link => "http://#{ENV["sserver_ip"]}:3333/rss_article_html/#{corpus.id}"
      }
    end

    def generate_twitter_article length, time, stream_id
      {
        :id => settings.global_article_counter.to_s,
        :text => get_corpus(length, :twitter).text,
        :created_at => time,
        :retweet_count => rand(30).round,
        :retweeted? => false,
        :favorited? => false,
        :media => [],
        :urls => [],
        :user_mentions => [],
        :hashtags => [],
        :pub_date => time #for stream server only
      }
    end

    def generate_facebook_article length, time, stream_id
      case settings.facebook_type_generator.pick
        when "status"
          {
            id: settings.global_article_counter.to_s,
            from: {
              name: "Max Mustermann#{settings.global_article_counter}",
              id: "123_#{settings.global_article_counter}"
            },
            message: get_corpus(length, :facebook).text,
            type: "status",
            status_type: "wall_post",
            created_time: time.to_s,
            updated_time: time.to_s,
            pub_date: time #for stream server only
          }
        when "photo"
          {
            "id" => settings.global_article_counter.to_s,
            "from" => {
              name: "Max Mustermann#{settings.global_article_counter}",
              id: "123_#{settings.global_article_counter}"
            },
            "message" => get_corpus(length, :facebook).text,
            "picture" => "https://fbcdn-photos-a-a.akamaihd.net/hphotos-ak-prn1/1012757_409348492516800_1425358490_s.jpg",
            "link" => "https://www.facebook.com/photo.php?fbid=409348492516800&set=a.307366282715022.71747.249418401843144&type=1&relevant_count=1",
            "type" => "photo",
            "status_type" => "added_photos",
            "object_id" => "409348492516800",
            "created_time" => "2013-06-24T10:00:01+0000",
            "updated_time" => "2013-06-24T10:00:01+0000",
            "shares" => {
              "count" => 1
            },
            "likes" => {
              "data" => [
                {
                  "name" => "Thorsten Runte",
                  "id" => "100000165801070"
                },
                {
                  "name" => "Christian Morl",
                  "id" => "100000611291816"
                },
                {
                  "name" => "Nadine Lanz",
                  "id" => "100001707658134"
                }
              ],
              "count" => 3
            },
            pub_date: time #for stream server only
          }
        when "link"
          {
            "id" => settings.global_article_counter.to_s,
            "from" => {
              name: "Max Mustermann#{settings.global_article_counter}",
              id: "123_#{settings.global_article_counter}"
            },
            "message" => get_corpus(length, :facebook).text,
            "picture" => "https://fbexternal-a.akamaihd.net/safe_image.php?d=AQDrPTq215zqvNy7&w=154&h=154&url=http%3A%2F%2Fsphotos-e.ak.fbcdn.net%2Fhphotos-ak-ash3%2F1013068_547380265298127_228272219_n.jpg",
            "link" => "http://www.stadtfest-badreichenhall.de/weblog-stadtfest-badreichenhall/index.php?action=view&offset=1&id=0",
            "name" => "weblog stadtfest bad reichenhall",
            "caption" => "www.stadtfest-badreichenhall.de",
            "description" => "www.stadtfest-badreichenhall.de",
            "icon" => "https://fbstatic-a.akamaihd.net/rsrc.php/v2/yD/r/aS8ecmYRys0.gif",
            "type" => "link",
            "status_type" => "shared_story",
            "created_time" => "2013-06-24T01:03:59+0000",
            "updated_time" => "2013-06-24T01:03:59+0000",
            "shares" => {
              "count" => 2
            },
            "likes" => {
              "data" => [
                {
                  "name" => "Wolfgang Schaak",
                  "id" => "100002224900912"
                }
              ],
              "count" => 1
            },
            pub_date: time #for stream server only
          }
        when "video"
          {
            "id" => settings.global_article_counter.to_s,
            "from" => {
              name: "Max Mustermann#{settings.global_article_counter}",
              id: "123_#{settings.global_article_counter}"
            },
            "message" => get_corpus(length, :facebook),
            "picture" => "https://fbexternal-a.akamaihd.net/safe_image.php?d=AQDOvqt4MjoTGOk9&w=130&h=130&url=http%3A%2F%2Fi3.ytimg.com%2Fvi%2Fj2IyX5LXGyg%2Fhqdefault.jpg%3Ffeature%3Dog",
            "link" => "http://www.youtube.com/watch?feature=player_embedded&v=j2IyX5LXGyg",
            "source" => "http://www.youtube.com/v/j2IyX5LXGyg?version=3&autohide=1&autoplay=1",
            "name" => "Airbourne - Runnin' Wild",
            "caption" => "www.youtube.com",
            "description" => "2008 WMG Airbourne - Purchase Runnin' Wild from iTunes - http://bit.ly/cZrvNp Airbourne - Runnin' Wild",
            "icon" => "https://fbstatic-a.akamaihd.net/rsrc.php/v2/yj/r/v2OnaTyTQZE.gif",
            "type" => "video",
            "status_type" => "shared_story",
            "created_time" => "2013-06-24T07:53:52+0000",
            "updated_time" => "2013-06-24T09:05:23+0000",
            "shares" => {
              "count" => 2
            },
            "likes" => {
              "data" => [
                {
                  "name" => "Brigitte Wagner",
                  "id" => "100000821062543"
                }
              ],
              "count" => 199
            },
            pub_date: time #for stream server only
          }
        when "checkin"
          {
            id: settings.global_article_counter.to_s,
            from: {
              name: "Max Mustermann#{settings.global_article_counter}",
              id: "123_#{settings.global_article_counter}"
            },
            message: get_corpus(length, :facebook),
            type: "status",
            status_type: "wall_post",
            created_time: time.to_s,
            updated_time: time.to_s,
            place: "Franz-Josef-Kai 7, 5020 Salzburg, Austria",
            pub_date: time #for stream server only
          }
      end
    end

    def generate_article length, time, stream_id, type
      settings.global_article_counter += 1
      self.send("generate_#{type}_article", length, time, stream_id)
    end

    def pick_body_length stream
      settings.send("#{stream[:type]}_body_length_generator").pick
    end

    def pick_publish_interval stream
      settings.send("#{stream[:type]}_publish_interval_generator").pick
    end

    def generate_articles stream_id
      result = []
      pub_date = 0
      stream = Cache.instance.get_stream stream_id

      throw "stream #{stream_id} not in cache!" unless stream

      if stream[:length] == 0.0
        return
      end

      # initialize stream on first load with full length
      unless stream[:next_pub_date]
        next_pub_date = Time.now
        stream[:length].to_i.times do |i|
          body_length = pick_body_length(stream)
          stream[:articles] << generate_article(body_length, next_pub_date, stream_id, stream[:type])
          next_pub_date -= pick_publish_interval(stream) * SCALE_FACTOR
        end
        # save most recent pub date as next_pubdate
        stream[:next_pub_date] = stream[:articles].first[:pub_date] + pick_publish_interval(stream) * SCALE_FACTOR
      else
        #on consecutive loads check if enough time passed and generate new articles
        next_publish_time = stream[:next_pub_date]
        while(next_publish_time < Time.now) do
          body_length = pick_body_length(stream)
          stream[:articles].unshift(generate_article(body_length, next_publish_time, stream_id, stream[:type]))
          stream[:articles].pop #remove last article to maintain constant length

          next_publish_time += pick_publish_interval(stream) * SCALE_FACTOR
          stream[:next_pub_date] = next_publish_time
        end
      end
    end

    get '/rss_article_html/:id' do
      id = params[:id].to_i
      corpus = Corpus.find(id)

      erb :article, :locals => { :corpus => corpus }
    end

    get '/rss/:id' do
      id = params[:id].to_i
      stream = Cache.instance.get_stream id
      generate_articles(id)
      builder :stream, :locals => { :articles => stream[:articles], :id => "#{id}. Next publication is: #{stream[:next_pub_date]}" }
    end

    get '/facebook/:id' do
      content_type :json

      id = params[:id]
      stream = Cache.instance.get_stream id
      generate_articles(id)
      stream[:articles].to_json
    end

    get '/twitter/:id/:since_id' do
      content_type :json

      id = params[:id]
      stream = Cache.instance.get_stream id
      generate_articles(id)

      # emulate twitter since_id: only respond with tweets whose id is greater than the since_id
      stream[:articles].select do |a|
        a[:id].to_i > params[:since_id].to_i
      end.to_json
    end
  end
end