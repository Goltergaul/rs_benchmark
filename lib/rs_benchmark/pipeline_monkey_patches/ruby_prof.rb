require "ruby-prof"
EventMachine.threadpool_size = 1 # better for profiling

Workers::BasicWorker.class_eval do
  alias_method :original_run!, :run!
  alias_method :original_on_shutdown, :on_shutdown
  alias_method :original_get_async_consume_proc, :get_async_consume_proc

  def run!
    RubyProf.start
    original_run!
  end

  def on_shutdown
    on_shutdown_proc = original_on_shutdown()

    Proc.new do
      result = RubyProf.stop
      printer = RubyProf::CallTreePrinter.new(result)
      output_file = File.open("#{ENV["RAILS_ROOT"]}/profiling_output/#{self.class.name}", "w")
      printer.print(output_file)

      on_shutdown_proc.call
    end
  end

  def get_async_consume_proc payload
    # RubyProf.resume
    original_get_async_consume_proc(payload)
  end
end