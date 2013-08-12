require "gsl"
require "bigdecimal"

module RsBenchmark

  # Random generator that uses the "interval-method"
  class PseudoRandomGenerator

    def initialize distribution, seed=rand
      @distribution = {}
      distribution.each do |value, probabillity|
        @distribution[value] = BigDecimal.new(probabillity.to_s)
      end
      @rnd = GSL::Rng.alloc("gsl_rng_mt19937", seed)
    end

    # pick a value from the distribution
    def pick
      float = BigDecimal.new(@rnd.uniform.to_s)
      from = to = BigDecimal.new("0.0")
      value = nil
      @distribution.each do |value, probabillity|
        to += probabillity
        if float >= from && float < to
          return value
        end
        from = to
      end

      throw "probabillity sum not 1.0, was #{to} for #{@distribution}" if to < 0.999
      return value
    end

  end

  # Random generator that uses the "Urn-Method"
  class UrnRandomGenerator

    # needs absolute frequencies, not relative ones
    def initialize abs_distribution, seed=rand
      @distribution = abs_distribution

      # check distribution
      @distribution.values.each do |value|
        throw "First param passed to UrnRandomGenerator must be absolute frequencies (the value of each pair must be an integer!) Value was #{value}" unless value.is_a? Integer
      end

      @gcd = @distribution.values.first
      @distribution.values.each do |val|
        @gcd = @gcd.gcd(val)
      end

      @rnd = GSL::Rng.alloc("gsl_rng_mt19937", seed)
      @urn_content = []
    end

    # initially fills the urn with values according to the distribution
    def fill_urn
      @distribution.each do |value, count|
        (count/@gcd).to_i.times do
          @urn_content << value
        end
      end
    end

    # pick a value. If the urn is empty fill it.
    def pick
      if @urn_content.length == 0
        fill_urn
      end

      @urn_content.delete_at((@rnd.uniform*(@urn_content.length-1)).round)
    end

  end
end