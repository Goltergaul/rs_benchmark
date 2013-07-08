module WorkloadInducer
  class Task
    def initialize(options, next_task = nil)
      throw "wait_time missing" unless options[:wait_time]
      @wait_time = options[:wait_time]
      @next_task = next_task
      @run = false
    end

    def wait_time
      @wait_time
    end

    # execute task
    def perform
      throw "This should be overriden"
    end

    def remaining_task_count
      count = 0
      count += @next_task.remaining_task_count if @next_task

      return (@run ? 0 : 1) + count
    end

    def next_task=task
      @next_task = task
    end
  end

  class UserScheduleTask < Task

    def initialize(options, next_task = nil)
      super(options, next_task)
      @user_id = options[:user_id]
    end

    def perform chain_finished_callback
      user = User.find(@user_id)
      user.current_sign_in_at = Time.now
      @run = true

      if @next_task
        # puts "scheduling next UserScheduleTask for in #{@wait_time} seconds (#{(@wait_time/60).round(2)} minutes"
        next_task = Inducer.get_next_task
        if Time.now + @wait_time < next_task || next_task <= Time.now
          Inducer.set_next_task Time.now + @wait_time
        end
        EM.add_timer @wait_time, proc {
          @next_task.perform(chain_finished_callback)
        }
      else
        chain_finished_callback.call
      end
    end
  end
end