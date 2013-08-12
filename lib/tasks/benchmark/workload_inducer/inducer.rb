require_relative "task"
require "gsl"

module WorkloadInducer
  class Inducer
    include Singleton

    def self.get_next_task
      @@next_task
    end

    def self.set_next_task value
      @@next_task = value
    end

    def initialize
      @chains = []
    end

    def add_chain task_chain
      @chains << task_chain
    end

    def on_shutdown
      Proc.new do
        puts "Caught TERM signal, stopping EM"
        @stream_server_thread.kill
        EM.stop
      end
    end

    # start inducing workload, run all chains simultaneous
    def induce!(stream_server_thread)

      @chain_count = @chains.count
      @task_count = @chains.sum(&:remaining_task_count)
      @stream_server_thread = stream_server_thread
      @@next_task = Time.now
      @rand = GSL::Rng.alloc("gsl_rng_mt19937", BenchmarkStreamServer::SEED)
      @steps_done = 0

      Signal.trap "INT", on_shutdown
      Signal.trap "TERM", on_shutdown

      puts "Starting Inducer EM at #{Time.now} ..."
      EM::run do
        puts "Starting Chains..."
        @chains_started_count = 0

        unless ENV["ramp_up_delay"]
          @chains.each do |task|
            delay_time = task.wait_time + (@rand.uniform*(task.wait_time-1)).to_i
            puts "Delaying chain start by #{(delay_time/60.0).round(2)} minutes"
            start_chain task, delay_time
          end
        else
          puts "Using ramp up mode with #{ENV["ramp_up_delay"]} seconds delay"
          ramp_up_stepsize = ENV["ramp_up_step"].nil? ? 1 : ENV["ramp_up_step"].to_i
          @chains.each_slice(ramp_up_stepsize).with_index do |tasks, index|
            next if index*ramp_up_stepsize >= ENV["user_count"].to_i
            EM.add_timer ENV["ramp_up_delay"].to_i*index, proc {
              puts "\nNext ramp step reached. Starting another #{ramp_up_stepsize} chains"
              @steps_done += ramp_up_stepsize

              RsBenchmark::ResponseTime::RsBenchmarkResponseTime.create!(
                :tag => "ramp_up_step",
                :data => {
                  :time => Time.now,
                  :step_size => ramp_up_stepsize,
                  :new_step_count => @steps_done,
                  :total_step_count => @chain_count
                }
              )

              tasks.each do |task|
                # start task within at max half of the ramp up delay
                start_chain task, rand(ENV["ramp_up_delay"].to_i/2)
              end
            }
          end
        end

        print_status
      end
    end

    def start_chain task, delay_time=0
      chain_finished_callback = proc {
        puts "*************************************"
        puts "Chain finished at #{Time.now}!"
        puts "*************************************"
        remove_chain(task)
      }

      # starting chains with different delays so that not everything is at the same time
      EM.add_timer delay_time, proc {
        # puts "Starting Chain!"
        @chains_started_count += 1
        task.perform(chain_finished_callback)

        if @chains_started_count == @chains.count
          puts "*************************************"
          puts "ALL CHAINS STARTED AT #{Time.now}"
          puts "*************************************"
        end
      }
    end

    def print_status
      next_run = ((@@next_task-Time.now)/60.0).round(2)

      print "\r"
      print "#{Time.now} #{[@chains.count,@steps_done].min}/#{@chain_count} chains active - #{@chains.sum(&:remaining_task_count)}/#{@task_count} Tasks remaining - Next task will run in #{next_run} minutes"

      EM.add_timer 1, proc {
        print_status
      }
    end

    def remove_chain task_chain
      @chains.delete(task_chain)

      if @chains.length == 0
        puts "Stopping Inducer at #{Time.now}"
      end
    end
  end
end