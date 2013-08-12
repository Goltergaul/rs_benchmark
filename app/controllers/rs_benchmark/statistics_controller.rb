#encoding: utf-8
require "lazy_high_charts"
require "gsl"

module RsBenchmark

  class StatisticsController < RsBenchmark::ApplicationController
    layout "stats"
    http_basic_authenticate_with :name => Engine.benchmark_config[:access_control][:user], :password => Engine.benchmark_config[:access_control][:password]

    def benchmark_results
      from_time = params[:from] ? DateTime.parse(params[:from]).utc : DateTime.parse("1.1.1999").utc
      to_time = params[:to] ? DateTime.parse(params[:to]).utc : Time.now.utc

      # antwortzeiten diagramm

      map = %Q{
        function() {
          if(this.tag.match(/^response_time/)) {
            var minute = (Math.ceil(this.data.time.getUTCMinutes()/10)*10); // build mean over every 10 minutes
            var time = new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), minute);
            time = new Date(time - 5 * 1000 * 60);
            emit(this.tag+time, {
              real_sum: this.data.real,
              real_count: 1,
              utime_sum: this.data.utime,
              utime_count: 1,
              stime_sum: this.data.stime,
              stime_count: 1,
              time: +time,
              tag: this.tag
            });
          } else {
            var minute = (Math.ceil(this.data.time.getUTCMinutes()/5)*5); // build mean over every 5 minutes
            var time = new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), minute);
            time = new Date(time - 2.5 * 1000 * 60);
            //var time = new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), this.data.time.getMinutes());
            emit(this.tag+time, {
              real_sum: this.data.real,
              real_count: 1,
              utime_sum: this.data.utime,
              utime_count: 1,
              stime_sum: this.data.stime,
              stime_count: 1,
              time: +time,
              tag: this.tag
            });
          }
        }
      }

      reduce = %Q{
        function(key, values) {
          var result = { real_sum: 0, real_count: 0, utime_sum: 0, utime_count: 0, stime_count: 0, stime_sum: 0, time: null, tag: null };
          values.forEach(function(value) {
              result.real_sum += value.real_sum;
              result.real_count += value.real_count;
              result.utime_sum += value.utime_sum;
              result.utime_count += value.utime_count;
              result.stime_sum += value.stime_sum;
              result.stime_count += value.stime_count;
              result.time = value.time;
              result.tag = value.tag;
          });
          return result;
        }
      }

      finalize  = %Q{
        function(key, value) {
          value.real_mean = value.real_sum/value.real_count;
          value.utime_mean = value.utime_sum/value.utime_count;
          value.stime_mean = value.stime_sum/value.stime_count;
          return value;
        }
      }

      # response times
      results = RsBenchmark::ResponseTime::RsBenchmarkResponseTime.between("data.time" => from_time..to_time)
        .where(:tag.in => [/^consume_/, "response_time_stream_first", "response_time_stream_all"])
        .where(:"data.real".lt => 1000).map_reduce(map, reduce).out(:inline => 1).finalize(finalize)

      @chart_data = {}
      results.each do |bm|
        @chart_data[bm["value"]["tag"]] = { "values_real" => [], "values_utime" => [], "values_stime" => []} unless @chart_data[bm["value"]["tag"]]
        @chart_data[bm["value"]["tag"]]["values_real"] << [bm["value"]["time"].to_i, bm["value"]["real_mean"]]
        @chart_data[bm["value"]["tag"]]["values_utime"] << [bm["value"]["time"].to_i, bm["value"]["utime_mean"]]
        @chart_data[bm["value"]["tag"]]["values_stime"] << [bm["value"]["time"].to_i, bm["value"]["stime_mean"]]
      end
      @chart_data.each do |key, data|
        ["values_real","values_utime","values_stime"].each do |values_type|
          data[values_type] = data[values_type].sort_by { |k| k[0] } # sort by time
          values_array = data[values_type].map do |a|
            a[1]
          end
          vector = GSL::Vector.alloc(values_array)
          data["mean_#{values_type}"] = vector.mean
          data["sd_#{values_type}"] = vector.sd
          data["median_#{values_type}"] = vector.median_from_sorted_data
          data["quantil_90_#{values_type}"] = calculate_percentile(values_array, 0.9)
        end
      end

      # user counts
      ramp_up_steps = RsBenchmark::ResponseTime::RsBenchmarkResponseTime.where(:tag => "ramp_up_step").between("data.time" => from_time..to_time).asc("data.time")
      user_counts = []
      last_step_count = 0
      ramp_up_steps.each do |bm|
        user_counts << [bm.data["time"].utc.to_i*1000, last_step_count]
        user_counts << [bm.data["time"].utc.to_i*1000, bm.data["new_step_count"]]
        last_step_count = bm.data["new_step_count"]
      end

      # POFOD
      map = %Q{
        function() {
          var minute = (Math.ceil(this.data.time.getUTCMinutes()/5)*5); // build mean over every 5 minutes
          var time = new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), minute);
          time = new Date(time - 2.5 * 1000 * 60);
          if(this.tag == "failure") {
            emit(time.toString(), {
              failure_count: 1,
              success_count: 0,
              time: time
            });
          } else {
            emit(time.toString(), {
              failure_count: 0,
              success_count: 1,
              time: time
            });
          }
        }
      }

      reduce = %Q{
        function(key, values) {
          var result = { time: null, failure_count: 0, success_count: 0 };
          values.forEach(function(value) {
              result.failure_count += value.failure_count;
              result.success_count += value.success_count;
              result.time = value.time;
          });
          return result;
        }
      }

      finalize  = %Q{
        function(key, value) {
          value.failure_ratio = value.failure_count/value.success_count;
          return value;
        }
      }

      failures_successes = RsBenchmark::ResponseTime::RsBenchmarkResponseTime.where(:tag.in => ["failure","success"]).between("data.time" => from_time..to_time)
        .map_reduce(map, reduce).out(:inline => 1).finalize(finalize)
      failure_ratio = []
      failures_successes.each do |bm|
        failure_ratio << [bm["value"]["time"].utc.to_i * 1000, bm["value"]["failure_ratio"]]
      end
      failure_ratio = failure_ratio.sort_by { |k| k[0] } # sort by time

      # overall pofod
      success_count = RsBenchmark::ResponseTime::RsBenchmarkResponseTime.where(:tag => "success").between("data.time" => from_time..to_time).count
      failure_count = RsBenchmark::ResponseTime::RsBenchmarkResponseTime.where(:tag => "failure").between("data.time" => from_time..to_time).count
      @pofod = failure_count/success_count.to_f


      # IO Rate
      map = %Q{
        function() {
          var time = new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), this.data.time.getMinutes());
          if((this.data.worker == "Workers::Rank" && this.tag == "success") ||
            this.tag == "failure" && (this.data.worker != "Workers::StreamFetcher" && this.data.worker != "Workers::ExistenceChecker")) {
            // this counts as output (when successful output of ranker or failure in all but stream fetcher & existence checker)
            emit(time.toString(), {
              count_in: 0,
              count_out: 1,
              type: this.tag,
              time: time
            });
          }

          if(this.tag == "success" && this.data.worker == "Workers::ExistenceChecker") {
          //if(this.tag == "enqueue")
            // this counts as input
            emit(time.toString(), {
              count_in: 1,
              count_out: 0,
              type: this.tag,
              time: time
            });
          }
        }
      }

      reduce = %Q{
        function(key, values) {
          var result = { time: null, count_in: 0, count_out: 0 };
          values.forEach(function(value) {
            result.count_in += value.count_in;
            result.count_out += value.count_out;
            result.time = value.time;
          });
          return result;
        }
      }

      accumulated_count_in = 0
      accumulated_count_out = 0
      @max_sum_out_per_minute = 0
      input_rate = RsBenchmark::ResponseTime::RsBenchmarkResponseTime.where(:tag.in => ["success","enqueue","failure"])
        .between("data.time" => from_time..to_time).map_reduce(map, reduce).out(:inline => 1).map do |bm|
          @max_sum_out_per_minute = bm["value"]["count_out"] if @max_sum_out_per_minute < bm["value"]["count_out"]
          [bm["value"]["time"].utc.to_i * 1000, bm["value"]["count_in"], bm["value"]["count_out"]]
      end

      input_rate = input_rate.sort_by { |k| k[0] } # sort by time
      input_rate.each do |value|
        accumulated_count_out += value[2]
        value[2] = accumulated_count_out

        accumulated_count_in += value[1]
        value[1] = accumulated_count_in
      end

      # Throughputs
      @throughputs = {}

      map = %Q{
        function() {
          /*var minute = (Math.ceil(this.data.time.getUTCMinutes()/5)*5); // build mean over every 5 minutes
          var time = new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), minute);
          time = new Date(time - 2.5 * 1000 * 60);*/

          var time = new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), this.data.time.getMinutes());
          emit(this.data.worker+time.toString(), {
            count: 1,
            worker: this.data.worker,
            time: time
          });
        }
      }

      reduce = %Q{
        function(key, values) {
          var result = { time: null, worker: null, count: 0 };
          values.forEach(function(value) {
            result.count += value.count;
            result.time = value.time;
            result.worker = value.worker;
          });
          return result;
        }
      }

      throughput_values = RsBenchmark::ResponseTime::RsBenchmarkResponseTime.where(:tag => "success")
        .between("data.time" => from_time..to_time).map_reduce(map, reduce).out(:inline => 1).each do |bm|
        @throughputs[bm["value"]["worker"]] = [] unless @throughputs[bm["value"]["worker"]]
        @throughputs[bm["value"]["worker"]] << [bm["value"]["time"].utc.to_i * 1000, bm["value"]["count"]]
      end

      @chart_response_times = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Antwortzeiten"})
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:type] = "datetime"
        f.options[:chart][:height] = "750"
        f.options[:plotOptions] = {
          :series => {
            :marker => {
              radius: 0
            }
          }
        }

        f.options[:yAxis] = [{
          title: {
            text: 'Response Time [s]'
          },
          min: 0,
          plotLines: [],
          #max: 2.5
        },
        {
          title: {
            text: 'Anzahl NutzerInnen'
          },
          min: 0
        },
        {
          title: {
            text: 'Streamverarbeitungsdauer [s]'
          },
          min: 0,
          #max: 500
        },
        {
          title: {
            text: 'Fehlerrate'
          },
          min: 0,
          #max: 0.016
        },
        {
          title: {
            text: 'Anzahl an Artikeln in Verarbeitung'
          },
          min: 0
        },
        {
          title: {
            text: 'Throughput [pro 5 Minuten]'
          },
          min: 0,
          #max: 2000
        }]

        f.series(:type=> 'arearange',:name=> "Anzahl an Artikeln in Verarbeitung (Fläche) Oberkante = Anzahl zu verarbeitender Artikel, Unterkante = Verarbeitete Artikel",
            :data => input_rate, :yAxis => 4, :fillOpacity => 0.2,
            :lineWidth => 1, :color => "rgba(0,0,255,0.5)")

        f.series(:type=> 'line',:name=> "Useranzahl",
            :data => user_counts, :yAxis => 1,
            :lineWidth => 1, :color => "#000000")

        f.series(:type=> 'line',:name=> "Fehlerrate",
            :data => failure_ratio, :yAxis => 3,
            :lineWidth => 1, :color => "#ff0000", :visible => false)

        colors = ["#ffca00", "#47ff00", "#00ffff", "#00a5ff", "#c300ff", "#ff00e1"]

        @chart_data.each do |key, data|
          ["values_real","values_utime","values_stime"].each do |values_type|
            f.series(:type=> 'spline',:name=> "#{key} #{values_type}",
              :data => data[values_type], :yAxis => key.match(/response/)? 2 : 0,
              :lineWidth => key.match(/response/)? 2 : 1, :color => colors.pop, :visible => false)

            # if key == "consume_Workers::Rank"
            #  f.options[:yAxis][0][:plotLines] << {
            #     value: data["quantil_90"],
            #     color: '#ff0000',
            #     width:2,
            #     zIndex:4,
            #     label:{text:"90%-Quantil #{data["quantil_90_#{values_type}"].round(2)}"}
            #   }

            #   f.options[:yAxis][0][:plotLines] << {
            #     value: data["mean"],
            #     color: '#ff0000',
            #     width:2,
            #     zIndex:4,
            #     label:{text:"Durchschnitt #{data["mean_#{values_type}"].round(2)}"}
            #   }
            # end
          end
        end

        colors = ["#ffca00", "#47ff00", "#00ffff", "#00a5ff", "#c300ff", "#ff00e1"]
        @throughputs.each do |key, values|
          f.series(:type=> 'spline',:name=> "throughput #{key}",
              :data => values, :yAxis => 5,
              :lineWidth => 1, :color => colors.pop, :visible => false)
        end
      end

      # histogramm feedverarbeitungsdauer (erster artikel)
      result = Statistics::Dayly.histogramm(RsBenchmark::ResponseTime::RsBenchmarkResponseTime.where("tag" => "response_time_stream_first").where(:"data.real".lt => 999), "real", 20, "data")

      @histogram_feed_first_arrival_times = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Response Time Stream Verarbeitung bis zum ersten Artikel"})
        f.options[:xAxis][:type] = "linear"
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:categories] = result[:labels]
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right' }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0
          }]

        f.series(:type=> 'column',:name=> 'Verarbeitungsdauern in Sekunden',
          :data => result[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })
      end

      # histogramm feedverarbeitungsdauer (letzter artikel)
      result = Statistics::Dayly.histogramm(RsBenchmark::ResponseTime::RsBenchmarkResponseTime.where("tag" => "response_time_stream_all").where(:"data.real".lt => 999), "real", 20, "data")

      @histogram_feed_last_arrival_times = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Response Time Stream Verarbeitung bis zum letzten Artikel"})
        f.options[:xAxis][:type] = "linear"
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:categories] = result[:labels]
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right' }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0
          }]

        f.series(:type=> 'column',:name=> 'Verarbeitungsdauern in Sekunden',
          :data => result[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })
      end

    end

    def index
      # gather statistics used for various diagramms
      user_data = {}

      User.all.each do |user|
        user_data.deep_merge!({"user_profile" => {user.id.to_s => {"global_stream_count" => user.global_streams.count, "private_streams_count" => user.authentications.count}}})
      end


      grouped_by_user = RsBenchmark::Data.get_intervals_grouped_by_user_id("reschedule_stream_updates")
      grouped_by_user.each do |user_id, data|
        user = User.where(:id => user_id).first
        next unless user
        next if user.current_sign_in_at - user.created_at < 2.weeks
        next if data[:intervals].count < 10 # use only users that logged in at least 20 times for more representative data

        user_data.deep_merge!({"user_profile" => {user_id.to_s => {"reschedule_intervals" => data[:intervals]}}})
      end


      grouped_by_user = {}
      Rating.all.each do |rating|
        grouped_by_user[rating.user_id.to_s] = {:times => [], :intervals => []} unless grouped_by_user[rating.user_id.to_s]
        grouped_by_user[rating.user_id.to_s][:times] << rating.created_at
      end

      grouped_by_user.each do |user_id, data|
        times = data[:times].sort
        times.each_with_index do |time, index|
          if index >= 1
            value1 = times[index-1]
            value2 = time
            grouped_by_user[user_id][:intervals] << (value2-value1)/60.0
          end
        end
        data[:intervals].delete(nil)
      end
      grouped_by_user

      mean_of_means_sum = 0
      mean_of_means_count = 0
      grouped_by_user.each do |user_id, data|
        next if data[:intervals].count < 25
        user = User.find(user_id)
        next if user.current_sign_in_at - user.created_at < 2.weeks
        mean = data[:intervals].sum/data[:intervals].count.to_f
        mean_of_means_sum += mean
        mean_of_means_count += 1
        user_data.deep_merge!({"user_voting" => {user_id => { "email" => user.email, "mean" => mean }}})
      end
      user_data.deep_merge!({"user_voting" => { "mean" => mean_of_means_sum/mean_of_means_count.to_f}})


      User.all.each do |user|
        next if user.current_sign_in_at - user.created_at < 2.weeks
        user_data.deep_merge!({"user_profile" => { user.id.to_s => {
          "word_count" => user.attribute_ratings.where(:kind => "word").count,
          "rated_entries" => Rating.where(:user_id => user.id).count
        }}})
      end

      user_data["user_profile"].each do |user_id, profile|
        user_data["user_profile"].delete(user_id) if !profile.has_key?("word_count") ||
          !profile.has_key?("rated_entries") || !profile.has_key?("reschedule_intervals") ||
          !profile.has_key?("global_stream_count") || !profile.has_key?("private_streams_count")
      end

      # ####### histogramm user anzahl an global streams
      g_stream_counts = user_data["user_profile"].map do |user_id, profile|
        profile["global_stream_count"]
      end
      result = Statistics::Dayly.histogramm_from_array(g_stream_counts, 20)

      sum = 0
      result[:data].each_with_index do |data, i|
        sum += data*result[:labels][i]
      end

      @user_stats_global_stream_counts = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm über die Anzahl an globalen Streams pro User"})
        f.options[:xAxis][:type] = "linear"
        f.options[:chart][:height] = "300"
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:plotLines] = [{
          value: sum/result[:data].sum.to_f,
          color: '#ff0000',
          width:2,
          zIndex:4,
          label:{text:"Durchschnitt #{(sum/result[:data].sum.to_f).round(2)}"}
        }]
        f.options[:xAxis][:categories] = result[:labels]
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right' }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0
          }]

        f.series(:type=> 'column',:name=> 'Häufigkeiten globaler Streams',
          :data => result[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })
      end

      puts "user glob stream fertig"

      # ####### histogramm user anzahl an privaten streams
      p_stream_counts = user_data["user_profile"].map do |user_id, profile|
        profile["private_streams_count"]
      end
      result = Statistics::Dayly.histogramm_from_array(p_stream_counts, 3)

      sum = 0
      result[:data].each_with_index do |data, i|
        sum += data*result[:labels][i]
      end

      @user_stats_private_stream_counts = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm über die Anzahl an privater Streams pro User"})
        f.options[:chart][:height] = "300"
        f.options[:xAxis][:type] = "linear"
        f.options[:xAxis][:plotLines] = [{
          value: sum/result[:data].sum.to_f,
          color: '#ff0000',
          width:2,
          zIndex:4,
          label:{text:"Durchschnitt #{(sum/result[:data].sum.to_f).round(2)}"}
        }]
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:categories] = result[:labels]
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right' }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0
          }]

        f.series(:type=> 'column',:name=> 'Häufigkeiten privater Streams',
          :data => result[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })

        f.series(:type=> 'pie',:name=> 'Verteilung Twitter/Facebook',
          :data=> [
            {:name=> 'Facebook', :y=> User.elem_match(:authentications => {:_type => "User::Authentication::Facebook"}).where(:authentications.with_size => 1).count, :color=> '#3b5998'},
            {:name=> 'Twitter', :y=> User.elem_match(:authentications => {:_type => "User::Authentication::Twitter"}).where(:authentications.with_size => 1).count, :color=> '#27cbfe'}
          ],
          :center=> [50, 50], :size=> 130, :showInLegend=> false, dataLabels: {
            enabled: true,
            formatter: "function() {
                return Math.round(this.percentage*100)/100 + '% <br>'+this.point.name;
            }".js_code,
            distance: -40,
            color:'white'
          })

        f.options[:labels] = {
          items: [{
            html: 'Typenverteilung für einen Stream',
            style: {
              left: '30px',
              top: '-13px',
              color: 'black'
            }
          }]
        }
      end

      puts "user private stream fertig"

      # histogramm abonnentenanzahl
      users = user_data["user_profile"].map do |user_id, profile|
        User.find(user_id)
      end

      global_streams = users.map do |u|
        u.global_streams
      end.flatten.uniq

      gs_stats = {}
      sum = 0
      global_streams.each do |s|
        gs_stats[s.user_ids.count] = 0 unless gs_stats[s.user_ids.count]
        gs_stats[s.user_ids.count] += 1
        sum += s.user_ids.count
      end

      gs_stats = gs_stats.sort_by { |key,value| -value }
      @histogramm_abo_count = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"AbonnentInnen Anzahlen pro Stream"})
        f.options[:chart][:height] = "300"
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:plotLines] = [{
          value: sum/global_streams.count.to_f,
          color: '#ff0000',
          width:2,
          zIndex:4,
          label:{text:"Durchschnitt #{(sum/global_streams.count.to_f).round(2)}"}
        }]
        f.options[:xAxis][:categories] = gs_stats.map do |array| array[0] end
        f.options[:xAxis][:type] = "linear"
        f.options[:xAxis][:title] = { text: "AbonnentInnen Anzahl" }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0
          }]

        f.series(:type=> 'column',:name=> 'Häufigkeit',
          :data => gs_stats.map do |array| array[1] end, :color => "#005fad", dataLabels: {
            enabled: true
          })
      end

      # login interval histogram
      @user_reschedule_statistics = RsBenchmark::Data.get_intervals_grouped_by_user_id("reschedule_stream_updates")
      intervals = user_data["user_profile"].sum do |user_id, data|
        data["reschedule_intervals"]
      end.collect { |n| n * 60 }.reject { |n| n < 1 }
      histogramm_data = Statistics::Dayly.histogramm_from_array(intervals, 50, true)

      @histogramm_user_reschedule_interval = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm Zeitraum zwischen Reschedules aller NutzerInnen"})
        f.options[:xAxis][:type] = "linear"
        f.options[:yAxis][:type] = "logarithmic"
        f.options[:xAxis][:tile] = "Intervale in Minuten"
        f.options[:xAxis][:categories] = histogramm_data[:labels]
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right' }

        f.series(:type=> 'column',:name=> "Häufigkeit",
          :data => histogramm_data[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })
      end

      puts "logininterval fertig"

      # diagram adding feeds
      grouped_by_user = {}
      last_time = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "reschedule_stream_updates").asc("data.time").last.data["time"]
      RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "reschedule_stream_updates").each do |bm|
        user = User.where(:id => bm.data["user_id"]).first
        next unless user
        grouped_by_user[bm.data["user_id"]] = {:last_count => 0, :points => [[user.created_at.utc.to_i*1000, 0]], :last_login => user.current_sign_in_at} unless grouped_by_user[bm.data["user_id"]]

        if grouped_by_user[bm.data["user_id"]][:last_count] != bm.data["global_streams_count"]
          grouped_by_user[bm.data["user_id"]][:last_count] = bm.data["global_streams_count"]
          grouped_by_user[bm.data["user_id"]][:points] << [bm.data["time"].utc.to_i*1000, grouped_by_user[bm.data["user_id"]][:last_count]]
        end
      end

      @adding_feeds = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Änderungen in Anzahl abonnierter Streams"})
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:type] = "datetime"
        f.options[:chart][:height] = "1000"
        f.options[:plotOptions] = {
          :series => {
            :marker => {
              radius: 0
            }
          }
        }

        f.options[:yAxis] = [{
            title: {
              text: 'Anzahl an abonnierten Streams'
            },
            min: 0
          }]

        grouped_by_user.each do |user_id, data|
          next if data[:points].count == 0
          f.series(:type=> 'line',:name=> user_id.to_s,
            :data => data[:points]+[[data[:last_login].utc.to_i*1000, data[:points].last[1]]] , :yAxis => 0, :lineWidth => 1)
        end
      end

      ###### histogramm artikellängen rss
      # result = Statistics::Dayly.histogramm(RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank", "data.service" => "rss"), "body_length", 2670, "data", false)
      body_lengths = Entry.where(:service => "rss").map do |entry|
        entry.body.nil? ? 0 : entry.body.length
      end
      result = Statistics::Dayly.histogramm_from_array(body_lengths, 100, true)
      @histogramm_rss_article_length = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm über die Anzahl an Zeichen pro RSS Artikel"})
        f.options[:xAxis][:type] = "linear"
        f.options[:chart][:height] = "200"
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:categories] = result[:labels]
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right', :style => {"font-size" => "15px"} }
        f.options[:legend] = {
          enabled: false
        }
        f.options[:plotOptions] = {
          series: {
            dataLabels: {
              rotation: -90, :align => 'left'
            }
          }
        }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0,
            style: {
              "font-size" => "15px"
            }
          }]

        f.series(:type=> 'column',:name=> 'Häufigkeiten der Artikellängen',
          :data => result[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })
      end

      puts "rss längen fertig"

      ####### histogramm artikellängen facebook
      # result = Statistics::Dayly.histogramm(RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank", "data.service" => "facebook"), "body_length", 2670, "data", false)
      body_lengths = Entry.where(:service => "facebook").map do |entry|
        entry.body.nil? ? 0 : entry.body.length
      end
      result = Statistics::Dayly.histogramm_from_array(body_lengths, 100, true)
      @histogramm_facebook_article_length = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm über die Anzahl an Zeichen pro Facbook Artikel"})
        f.options[:xAxis][:type] = "linear"
        f.options[:chart][:height] = "200"
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:categories] = result[:labels]
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right', :style => {"font-size" => "15px"} }
        f.options[:legend] = {
          enabled: false
        }
        f.options[:plotOptions] = {
          series: {
            dataLabels: {
              rotation: -90, :align => 'left'
            }
          }
        }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0,
            style: {
              "font-size" => "15px"
            }
          }]

        f.series(:type=> 'column',:name=> 'Häufigkeiten der Artikellängen',
          :data => result[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })
      end

      puts "facebook längen fertig"

      ####### histogramm artikellängen twitter
      # result = Statistics::Dayly.histogramm(RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank", "data.service" => "twitter"), "body_length", 100, "data", false)
      body_lengths = Entry.where(:service => "twitter").map do |entry|
        entry.body.nil? ? 0 : entry.body.length
      end
      result = Statistics::Dayly.histogramm_from_array(body_lengths, 50)
      @histogramm_twitter_article_length = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm über die Anzahl an Zeichen pro Twitter Artikel"})
        f.options[:xAxis][:type] = "linear"
        f.options[:chart][:height] = "200"
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:categories] = result[:labels]
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right', :style => {"font-size" => "15px"} }
        f.options[:legend] = {
          enabled: false
        }
        f.options[:plotOptions] = {
          series: {
            dataLabels: {
              rotation: -90, :align => 'left'
            }
          }
        }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0,
            style: {
              "font-size" => "15px"
            }
          }]

        f.series(:type=> 'column',:name=> 'Häufigkeiten der Artikellängen',
          :data => result[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })
      end

      puts "twitter längen fertig"

      ###### histogramm feedlängen

      # get average stream length of every stream
      gs_data = GlobalStream::Rss.all.map do |gs|
        lengths = RsBenchmark::Logger::RsBenchmarkLogger.where("event" => "worker_stream_fetcher", "data.stream" => gs.url).map do |le|
          le.data["stream_entries_count"]
        end
        # build average
        if lengths.count == 0
          puts "no lengths for #{gs.url} found, skipping"
          next
        else
          lengths.sum/lengths.count.to_f
        end
      end
      result = Statistics::Dayly.histogramm_from_array(gs_data, 30)

      @histogramm_rss_stream_length = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm der verarbeiteten Streamlängen"})
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:categories] = result[:labels]
        f.options[:chart][:height] = "300"
        f.options[:xAxis][:type] = "linear"
        f.options[:xAxis][:title] = { text: "Streamlänge (Anzahl an Artikeln in Stream)" }
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right', }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0
          }]

        f.series(:type=> 'column',:name=> 'Häufigkeit der Streamlänge',
          :data => result[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })
      end

      puts "feed längen fertig"

      ####### pie chart facebook post types

      status_count = Entry::Facebook::Status.count
      checkin_count = Entry::Facebook::Checkin.count
      link_count = Entry::Facebook::Link.count
      photo_count = Entry::Facebook::Photo.count
      video_count = Entry::Facebook::Video.count

      @facebook_post_types_pie = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Häufigkeiten der verschiedenen Facebook Post Typen"})
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:categories] = result[:labels]
        f.options[:xAxis][:type] = "linear"
        f.options[:xAxis][:title] = { text: "Streamlänge (Anzahl an Artikeln in Stream)" }
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right', }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0
          }]

        f.series(:type=> 'pie',:name=> 'Facebook Post Typen',
          :data => [["Status", status_count], ["Checkins", checkin_count], ["Links", link_count], ["Fotos", photo_count], ["Videos", video_count]], :color => "#005fad", dataLabels: {
            enabled: true
          })
      end
    end

    def stream_publish_intervals

      ["rss", "twitter", "facebook"].each do |service|
        publish_intervals = []
        Statistics::Dayly.where("value.type" => "stream_publish_rate", "value.service" => service).each do |stream|
          times = stream["value"]["publish_times"].sort
          times.each_with_index do |time, index|
            if index >= 1
              value1 = times[index-1]
              value2 = time
              diff = (value2-value1)
              publish_intervals << diff if diff
            end
          end
        end
        publish_intervals.delete(0.0)
        puts publish_intervals.sort.inspect

        result = Statistics::Dayly.histogramm_from_array(publish_intervals, 50, true)

        highcharts_data = LazyHighCharts::HighChart.new('graph') do |f|
          f.title({ :text=>"Histogramm Zeitraum zwischen Veröffentlichungen eines #{service} Artikels innerhalb eines Streams"})
          f.options[:xAxis][:type] = "linear"
          f.options[:chart][:height] = "200"
          f.options[:chart][:zoomType] = "x"
          f.options[:xAxis][:categories] = result[:labels]
          f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right', :style => {"font-size" => "15px"} }
          f.options[:legend] = {
            enabled: false
          }
          f.options[:plotOptions] = {
            series: {
              dataLabels: {
                rotation: -90, :align => 'left'
              }
            }
          }
          f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0,
            style: {
              "font-size" => "15px"
            }
          }]

          f.series(
            :type=> 'column',
            :name=> 'Häufigkeiten',
            :data => result[:data],
            :color => "#005fad",
            dataLabels: {
              enabled: true
            }
          )
        end
        instance_variable_set "@histogramm_#{service}_publish_interval", highcharts_data
      end
    end
  end
end
