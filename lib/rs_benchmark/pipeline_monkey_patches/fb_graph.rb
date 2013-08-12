# This monkey patch is used to fetch facebook streams from the stream simulation server instead from facebook.com.
# Please note that the ip address of the simulation server is hardcoded into this file.

require "open-uri"

module FbGraph
  # Mock of FbGraph::User class that fetches its home feed from the workload generator instead from facebook
  class FbGraphUserMock
    def initialize(access_token)
      @access_token = access_token
    end

    class FBPostMock
      def initialize raw_attributes
        @raw_attributes = raw_attributes.with_indifferent_access
      end

      def raw_attributes
        @raw_attributes
      end
    end

    def home(options)
      posts = JSON.parse(open("http://192.168.178.22:3333/facebook/#{@access_token}").read)
      posts.map do |p|
        FBPostMock.new(p)
      end
    end
  end

  # reopen User class to mock me method.
  User.class_eval do
    def self.me(access_token)
      FbGraphUserMock.new(access_token)
    end
  end
end

module Workers
  class StreamFetcher
    Facebook.class_eval do
      # fixme this should be removed in production code anyway (do so before testing!)
      def self.get_and_merge_notifications user, fb_user, fb_uid, stream
      end
    end
  end
end