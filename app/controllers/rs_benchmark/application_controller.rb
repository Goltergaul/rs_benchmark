module RsBenchmark
  class ApplicationController < ActionController::Base
    protected
      def calculate_percentile(array, percentile)
        array.sort[(percentile * array.length).floor]
      end
  end
end
