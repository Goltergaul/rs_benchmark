Twitter::Client.class_eval do
  class TweetMock
    class UserMock
      def id
        "1334355"
      end

      def name
        "Max Musteruser#{rand(1000)}"
      end

      def screen_name
        "Max Musteruser#{rand(1000)}"
      end

      def profile_image_url
        "http://twitter.com"
      end
    end

    def initialize raw_attributes
      @raw_attributes = raw_attributes.with_indifferent_access
      @raw_attributes["user"] = UserMock.new
    end

    # alows acces to attribues in @raw_attributes via method calls (e.g. tweet.id)
    def method_missing(method, *args, &block)
      return @raw_attributes[method]
    end
  end

  def initialize options
    @access_token = options[:oauth_token]
  end

  def home_timeline options
    tweets = JSON.parse(open("http://192.168.178.22:3333/twitter/#{@access_token}/#{options[:since_id]}").read)
    tweets.map do |p|
      TweetMock.new(p)
    end
  end
end
