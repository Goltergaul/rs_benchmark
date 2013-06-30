require "gsl"

module RsBenchmark
  class PseudoRandomGenerator

    def initialize distribution, seed=rand
      @distribution = distribution
      @rnd = GSL::Rng.alloc("gsl_rng_mt19937", seed)
    end

    def pick
      float = @rnd.uniform
      from = to = 0.0
      total = 0
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

  class UrnRandomGenerator

    def initialize distribution, seed=rand
      @distribution = distribution
      @rnd = GSL::Rng.alloc("gsl_rng_mt19937", seed)
      @urn_content = []
    end

    def cycle_size
      minimal_probabillity = 1.0
      @distribution.each do |value, probabillity|
        minimal_probabillity = probabillity if probabillity < minimal_probabillity
      end

      100/minimal_probabillity
    end

    def fill_urn
      urn_size = cycle_size

      @distribution.each do |value, probabillity|
        (urn_size*probabillity).to_i.times do
          @urn_content << value
        end
      end
    end

    def pick
      if @urn_content.length == 0
        fill_urn
      end

      @urn_content.delete_at((@rnd.uniform*(@urn_content.length-1)).round)
    end

  end
end