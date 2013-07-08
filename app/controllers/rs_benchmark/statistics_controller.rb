#encoding: utf-8
require "lazy_high_charts"
require "gsl"

module RsBenchmark
  class StatisticsController < ActionController::Base
    layout "stats"
    http_basic_authenticate_with :name => Engine.benchmark_config[:access_control][:user], :password => Engine.benchmark_config[:access_control][:password]

    def benchmark_results
      from_time = params[:from] ? DateTime.parse(params[:from]).utc : DateTime.parse("1.1.1999").utc
      to_time = params[:to] ? DateTime.parse(params[:to]).utc : Time.now.utc

      # antwortzeiten diagramm

      map = %Q{
        function() {
          if(this.tag.match(/^response_time/)) {
            var minute = (Math.ceil(this.data.time.getUTCMinutes()/5)*5); // build mean over every 5 minutes
            var time = new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), minute);
            time = time.setMinutes(time.getMinutes() - 5);
            emit(this.tag+time, {
              real_sum: this.data.real,
              real_count: 1,
              utime_sum: this.data.utime,
              utime_count: 1,
              time: time,
              tag: this.tag
            });
          } else {
            emit(this.tag+this.data.time.getUTCDate()+this.data.time.getUTCHours()+this.data.time.getUTCMinutes(), {
              real_sum: this.data.real,
              real_count: 1,
              utime_sum: this.data.utime,
              utime_count: 1,
              time: +new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), this.data.time.getMinutes()),
              tag: this.tag
            });
          }
        }
      }

      reduce = %Q{
        function(key, values) {
          var result = { real_sum: 0, real_count: 0, utime_sum: 0, utime_count: 0, time: null, tag: null };
          values.forEach(function(value) {
              result.real_sum += value.real_sum;
              result.real_count += value.real_count;
              result.utime_sum += value.utime_sum;
              result.utime_count += value.utime_count;
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
          return value;
        }
      }

      # response times
      results = RsBenchmark::ResponseTime::RsBenchmarkResponseTime.between("data.time" => from_time..to_time)
        .where(:tag.in => [/^consume_/, "response_time_stream_first", "response_time_stream_all"])
        .where(:"data.real".lt => 1000).map_reduce(map, reduce).out(:inline => 1).finalize(finalize)

      @chart_data = {}
      results.each do |bm|
        @chart_data[bm["value"]["tag"]] = { "values" => []} unless @chart_data[bm["value"]["tag"]]
        @chart_data[bm["value"]["tag"]]["values"] << [bm["value"]["time"].to_i, bm["value"]["real_mean"]]
      end
      @chart_data.each do |key, data|
        data["values"] = data["values"].sort_by { |k| k[0] } # sort by time
        vector = GSL::Vector.alloc(data["values"].map do |a| a[1] end)
        data["mean"] = vector.mean
        data["sd"] = vector.sd
        data["median"] = vector.sort.median_from_sorted_data
      end

      # user numbers
      ramp_up_steps = RsBenchmark::ResponseTime::RsBenchmarkResponseTime.where(:tag => "ramp_up_step").between("data.time" => from_time..to_time).asc("data.time")
      user_counts = []
      last_step_count = 0
      ramp_up_steps.each do |bm|
        user_counts << [bm.data["time"].utc.to_i*1000, last_step_count]
        user_counts << [bm.data["time"].utc.to_i*1000, bm.data["new_step_count"]]
        last_step_count = bm.data["new_step_count"]
      end

      # failure/success rate
      map = %Q{
        function() {
          if(this.tag == "failure") {
            emit(this.data.time.getUTCDate()+this.data.time.getUTCHours()+Math.floor(this.data.time.getUTCMinutes()), {
              failure_count: 1,
              success_count: 0,
              type: this.tag,
              time: new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), this.data.time.getMinutes())
            });
          } else {
            emit(this.data.time.getUTCDate()+this.data.time.getUTCHours()+Math.floor(this.data.time.getUTCMinutes()), {
              failure_count: 0,
              success_count: 1,
              type: this.tag,
              time: new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), this.data.time.getMinutes())
            });
          }
        }
      }

      reduce = %Q{
        function(key, values) {
          var result = { time: null, type: null, failure_count: 0, success_count: 0 };
          values.forEach(function(value) {
              result.failure_count += value.failure_count;
              result.success_count += value.success_count;
              result.type = value.type;
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

      # io rate

      map = %Q{
        function() {
          var time = new Date(this.data.time.getFullYear(), this.data.time.getMonth(), this.data.time.getDate(), this.data.time.getHours(), this.data.time.getMinutes());
          if(this.data.worker == "Workers::Rank") {
            emit(this.data.time.getUTCDate()+this.data.time.getUTCHours()+this.data.time.getUTCMinutes(), {
              count_in: 0,
              count_out: 1,
              type: this.tag,
              time: time
            });
          } else {
            emit(this.data.time.getUTCDate()+this.data.time.getUTCHours()+this.data.time.getUTCMinutes(), {
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
      input_rate = RsBenchmark::ResponseTime::RsBenchmarkResponseTime.where(:tag => "success").where(:"data.worker".in => ["Workers::ExistenceChecker", "Workers::Rank"])
        .between("data.time" => from_time..to_time).map_reduce(map, reduce).out(:inline => 1).map do |bm|
          [bm["value"]["time"].utc.to_i * 1000, bm["value"]["count_in"], bm["value"]["count_out"]]
      end
      input_rate = input_rate.sort_by { |k| k[0] } # sort by time
      input_rate.each do |value|
        accumulated_count_out += value[2]
        value[2] = accumulated_count_out

        accumulated_count_in += value[1]
        value[1] = accumulated_count_in
      end

      # output_rate = RsBenchmark::ResponseTime::RsBenchmarkResponseTime.where(:tag => "success", :"data.type" => "Workers::Rank")
      #   .between("data.time" => from_time..to_time).map_reduce(map, reduce).out(:inline => 1).map do |bm|
      #     [bm["value"]["time"].utc.to_i * 1000, bm["value"]["success_count"]]
      # end

      @chart_response_times = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Antwortzeiten"})
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:type] = "datetime"
        f.options[:plotOptions] = {
          :series => {
            :marker => {
              radius: 0
            }
          }
        }
        f.options[:yAxis] = [{
          title: {
            text: 'Dauer [s]'
          },
          min: 0,
          # max: 10
        },
        {
          title: {
            text: 'Anzahl NutzerInnen'
          },
          min: 0
        },
        {
          title: {
            text: 'Feedverarbeitungsdauer [s]'
          },
          min: 0,
          # max: 400
        },
        {
          title: {
            text: 'Fehlerrate'
          },
          min: 0,
          # max: 400
        },
        {
          title: {
            text: 'Anzahl an Artikeln'
          },
          min: 0,
          # max: 400
        }]

        f.series(:type=> 'arearange',:name=> "Anzahl an Artikeln in Verarbeitung (Fläche) Oberkante = Anzahl zu verarbeitender Artikel, Unterkante = Verarbeitete Artikel",
            :data => input_rate, :yAxis => 4, :fillOpacity => 0.2,
            :lineWidth => 1, :color => "rgba(0,0,255,0.5)")

        f.series(:type=> 'line',:name=> "Useranzahl",
            :data => user_counts, :yAxis => 1,
            :lineWidth => 1, :color => "#0000ff")

        f.series(:type=> 'line',:name=> "Fehlerrate",
            :data => failure_ratio, :yAxis => 3,
            :lineWidth => 1, :color => "#ff0000")

        colors = ["#ffca00", "#47ff00", "#00ffff", "#00a5ff", "#c300ff", "#ff00e1"]
        @chart_data.each do |key, data|
          f.series(:type=> 'spline',:name=> key,
            :data => data["values"], :yAxis => key.match(/response/)? 2 : 0,
            :lineWidth => key.match(/response/)? 2 : 1, :color => colors.pop)
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

    def wichtig

      @user_reschedule_statistics = RsBenchmark::Data.get_mean_and_stdev_by_user("reschedule_stream_updates")

      # get intervals per user for rescheduling streams
      grouped_by_user = RsBenchmark::Data.get_intervals_grouped_by_user_id("reschedule_stream_updates")

      # render :text => grouped_by_user.inspect
      # return

      user_id = grouped_by_user.keys.first
      histogramm_data = Statistics::Dayly.histogramm_from_array(grouped_by_user[user_id][:intervals], 50, true)

      @histogramm_user_reschedule_interval_one_user = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm Zeitraum zwischen Reschedules der NutzerIn #{user_id}"})
        f.options[:xAxis][:type] = "linear"
        f.options[:yAxis][:type] = "logarithmic"
        f.options[:xAxis][:tile] = "Intervale in Minuten"
        f.options[:xAxis][:categories] = histogramm_data[:labels]
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right' }

        f.series(:type=> 'column',:name=> user_id,
          :data => histogramm_data[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })
      end

      @histogramm_user_reschedule_interval = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm Zeitraum zwischen Reschedules"})
        f.options[:chart] = {
          type: 'scatter',
          zoomType: 'xy',
          height: 2200
        }
        f.options[:xAxis] = [{
            title: {
              text: 'Intervalgröße in Minuten'
            },
            type: "logarithmic",
            startOnTick: true,
            endOnTick: true,
            showLastLabel: true
          }]
        f.options[:yAxis] = [{
            title: {
              text: 'Zufallszahlen'
            },
            #type: "logarithmic"
          }]
        f.options[:plotOptions] = {
          scatter: {
            tooltip: {
              headerFormat: '<b>Daten:</b><br>',
              pointFormat: 'Intervalgröße: {point.x}, Häufigkeit: {point.y}'
            }
          }
        }

        srand 234353564
        grouped_by_user.each_with_index do |hash, index|
          data = hash.last
          data_scatter = []
          next if hash.first == "51b059e319f1d790f5000047"
          data[:intervals].each_with_index do |x, i|
            data_scatter << [x, rand(10000)/100.0]
          end

          f.series(:name=> hash.first,
            :data => data_scatter, :color => "rgba(#{rand(255)}, #{rand(255)}, #{rand(255)}, .5)")
        end
      end

      puts "logininterval fertig"

      # ####### histogramm user anzahl an global streams
      result = Statistics::Dayly.histogramm(Statistics::Dayly.where("value.type" => "user_stats"), "global_streams_count", 20)
      sum = 0
      result[:data].each_with_index do |data, i|
        puts "#{data}*#{result[:labels][i]}"
        sum += data*result[:labels][i]
      end
      puts sum
      puts User.count

      @user_stats_global_stream_counts = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm über die Anzahl an globalen Streams pro User"})
        f.options[:xAxis][:type] = "linear"
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:categories] = result[:labels]
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right' }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0,
            plotLines:[{
              value: sum/User.count,
              color: '#ff0000',
              width:2,
              zIndex:4,
              label:{text:"Durchschnitt #{(sum/User.count).round(2)}"}
            }]
          }]

        f.series(:type=> 'column',:name=> 'Häufigkeiten globaler Streams',
          :data => result[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })
      end

      puts "user glob stream fertig"

      # ####### histogramm user anzahl an privaten streams
      result = Statistics::Dayly.histogramm(Statistics::Dayly.where("value.type" => "user_stats"), "private_streams_count", 20)

      @user_stats_private_stream_counts = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm über die Anzahl an privater Streams pro User"})
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

        f.series(:type=> 'column',:name=> 'Häufigkeiten privater Streams',
          :data => result[:data], :color => "#005fad", dataLabels: {
            enabled: true
          })

        f.series(:type=> 'pie',:name=> 'Verteilung Twitter/Facebook',
          :data=> [
            {:name=> 'Facebook', :y=> User.elem_match(:authentications => {:_type => "User::Authentication::Facebook"}).count, :color=> '#3b5998'},
            {:name=> 'Twitter', :y=> User.elem_match(:authentications => {:_type => "User::Authentication::Twitter"}).count, :color=> '#27cbfe'}
          ],
          :center=> [100, 80], :size=> 150, :showInLegend=> false, dataLabels: {
            enabled: true,
            formatter: "function() {
                return Math.round(this.percentage*100)/100 + '% <br>'+this.point.name;
            }".js_code,
            distance: -40,
            color:'white'
          })

        f.options[:labels] = {
          items: [{
            html: 'Typenverteilung',
            style: {
              left: '75px',
              top: '10px',
              color: 'black'
            }
          }]
        }
      end

      puts "user private stream fertig"

      ####### histogramm artikellängen rss
      result = Statistics::Dayly.histogramm(RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank", "data.service" => "rss"), "body_length", 150, "data", false)

      @histogramm_rss_article_length = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm über die Anzahl an Zeichen pro RSS Artikel"})
        f.options[:xAxis][:type] = "linear"
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
      result = Statistics::Dayly.histogramm(RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank", "data.service" => "facebook"), "body_length", 150, "data", false)

      @histogramm_facebook_article_length = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm über die Anzahl an Zeichen pro Facbook Artikel"})
        f.options[:xAxis][:type] = "linear"
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
      result = Statistics::Dayly.histogramm(RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank", "data.service" => "twitter"), "body_length", 150, "data", false)

      @histogramm_twitter_article_length = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm über die Anzahl an Zeichen pro Twitter Artikel"})
        f.options[:xAxis][:type] = "linear"
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

      ####### histogramm feedlängen

      result = Statistics::Dayly.histogramm(RsBenchmark::Logger::RsBenchmarkLogger.where("event" => "worker_stream_fetcher"), "stream_entries_count", 20, "data")

      @histogramm_rss_stream_length = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm der verarbeiteten Streamlängen"})
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
              publish_intervals << (value2-value1) if (value2-value1)
            end
          end
        end
        publish_intervals.delete(0.0)
        publish_intervals

        result = Statistics::Dayly.histogramm_from_array(publish_intervals, 45, true)

        highcharts_data = LazyHighCharts::HighChart.new('graph') do |f|
          f.title({ :text=>"Histogramm Zeitraum zwischen Veröffentlichungen eines #{service} Artikels innerhalb eines Streams"})
          f.options[:xAxis][:type] = "linear"
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

    def dayly_volume

      result = Statistics::Dayly.where("value.type" => "pipeline_overall")
      wsf = []
      wec = []
      wfd = []
      wsd = []
      wr = []
      timestamps = []

      result.each do |result|
        wsf << result["value"]["stream_fetcher_count"]
        wec << result["value"]["existence_checker_count"]
        wfd << result["value"]["fetch_details_count"]
        wsd << result["value"]["store_db_count"]
        wr << result["value"]["rank_count"]
        timestamps << result["value"]["time"]
      end

      @pipeline_all = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>""})
        f.options[:chart][:zoomType] = "x"
        f.options[:chart][:height] = "500"
        f.options[:xAxis][:type] = "datetime"
        f.options[:xAxis][:categories] = timestamps
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right' }
        f.options[:xAxis][:tickInterval] = 10
        f.options[:plotOptions] = {
          :series => {
            :marker => {
              radius: 2
            }
          },
          area: {
            stacking: 'normal',
            lineColor: '#666666',
            lineWidth: 1,
            marker: {
              enabled: false
            }
          }
        }

        f.options[:yAxis] = [{
            title: {
              text: 'Anzahl'
            },
            min: 0
          }]

        f.series(:type=> 'area',:name=> 'Stream Fetcher',
          :data => wsf, :yAxis => 0,
          :lineWidth => 1, :color => "#ff7f00")
        f.series(:type=> 'area',:name=> 'Existence Checker',
          :data => wec, :yAxis => 0,
          :lineWidth => 1, :color => "#0f0fff")
        f.series(:type=> 'area',:name=> 'Fetch Details',
          :data => wfd, :yAxis => 0,
          :lineWidth => 1, :color => "#7fff00")
        f.series(:type=> 'area',:name=> 'Store DB',
          :data => wsd, :yAxis => 0,
          :lineWidth => 1, :color => "#56ffff")
        f.series(:type=> 'area',:name=> 'Ranker',
          :data => wr, :yAxis => 0,
          :lineWidth => 1, :color => "#7f00ff")
      end

      ##artikel durchsatz durch komplette pipeline
      result = Statistics::Dayly.group_by_minute_and_hour "pipeline", Statistics::Dayly.where("value.type" => "pipeline")

      wsf = []
      wec = []
      wfd = []
      wsd = []
      wr = []

      result[:hourly].each do |result|
        wsf << result["stream_fetcher_count"]
        wec << result["existence_checker_count"]
        wfd << result["fetch_details_count"]
        wsd << result["store_db_count"]
        wr << result["rank_count"]
      end

      @pipeline = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Artikeldurchsatz durch die Pipeline im Tagesverlauf"})
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:type] = "datetime"
        f.options[:xAxis][:dateTimeLabelFormats] = {
          :day => '%H:%M',
        }
        f.options[:plotOptions] = {
          :series => {
            :marker => {
              radius: 1
            }
          },
          area: {
            stacking: 'normal',
            lineColor: '#666666',
            lineWidth: 1,
            marker: {
              enabled: false
            }
          }
        }
        f.options[:yAxis] = [{
          title: {
            text: 'Anzahl'
          },
          min: 0
        }]

        f.series(:type=> 'area',:name=> 'Stream Fetcher',
          :data => wsf, :pointStart => Date.today,
          :pointInterval => 1.day/1440, :yAxis => 0,
          :lineWidth => 1, :color => "#ff7f00")
        f.series(:type=> 'area',:name=> 'Existence Checker',
          :data => wec, :pointStart => Date.today,
          :pointInterval => 1.day/1440, :yAxis => 0,
          :lineWidth => 1, :color => "#0f0fff")
        f.series(:type=> 'area',:name=> 'Fetch Details',
          :data => wfd, :pointStart => Date.today,
          :pointInterval => 1.day/1440, :yAxis => 0,
          :lineWidth => 1, :color => "#7fff00")
        f.series(:type=> 'area',:name=> 'Store DB',
          :data => wsd, :pointStart => Date.today,
          :pointInterval => 1.day/1440, :yAxis => 0,
          :lineWidth => 1, :color => "#56ffff")
        f.series(:type=> 'area',:name=> 'Ranker',
          :data => wr, :pointStart => Date.today,
          :pointInterval => 1.day/1440, :yAxis => 0,
          :lineWidth => 1, :color => "#7f00ff")

      end

      ### artikel durchsatz durch rank worker über den tag hinweg

      data = Statistics::Dayly.all.to_a
      rss_count = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank").where("data.service" => "rss").count
      facebook_count = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank").where("data.service" => "facebook").count
      twitter_count = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank").where("data.service" => "twitter").count
      url_count = RsBenchmark::Logger::RsBenchmarkLogger.where(:event => "worker_rank").where("data.service" => "url").count

      result = Statistics::Dayly.group_by_minute_and_hour "rank_worker_stats", Statistics::Dayly.where("value.type" => "worker_rank")
      data_counts = result[:minutely].map do |result|
        result["article_count"]
      end
      data_counts_hourly = result[:hourly].map do |result|
        result["article_count"]
      end

      data_words = result[:minutely].map do |result|
        result["body_length"]
      end
      data_words_hourly = result[:hourly].map do |result|
        result["body_length"]
      end

      @chart_article_volume = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Artikeldurchsatz durch den Rank Worker im Tagesverlauf"})
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:type] = "datetime"
        f.options[:xAxis][:dateTimeLabelFormats] = {
          :day => '%H:%M',
        }
        f.options[:plotOptions] = {
          :series => {
            :marker => {
              radius: 1
            }
          }
        }
        f.options[:yAxis] = [{
            title: {
              text: 'Nachrichtenanzahl pro Minute'
            },
            min: 0
          },
          {
            title: {
              text: 'Wortanzahl pro Minute'
            },
            min: 0
          },
          {
            title: {
              text: 'Nachrichtenanzahl pro Stunde'
            },
            min: 0
          },
          {
            title: {
              text: 'Wortanzahl pro Stunde'
            },
            min: 0
          }]

          f.series(:type=> 'line',:name=> 'Nachrichten pro Minute',
            :data => data_counts, :pointStart => Date.today,
            :pointInterval => 1.day/1440, :yAxis => 0,
            :lineWidth => 1, :color => "#005fad")
          f.series(:type=> 'spline',:name=> 'Nachrichten pro Stunde',
            :data => data_counts_hourly, :pointStart => Date.today,
            :pointInterval => 1.day/24, :yAxis => 2,
            :lineWidth => 2, :color => "#006eff")
          f.series(:type=> 'line',:name=> 'Wörter pro Minute', :data => data_words,
           :pointStart => Date.today, :pointInterval => 1.day/1440, :yAxis => 1,
           :lineWidth => 1, :color => "#d34c5b")
          f.series(:type=> 'spline',:name=> 'Wörter pro Stunde', :data => data_words_hourly,
            :pointStart => Date.today, :pointInterval => 1.day/24,
            :yAxis => 3, :lineWidth => 2, :color => "#e21444")

          f.series(:type=> 'pie',:name=> 'Service distribution',
            :data=> [
              {:name=> 'Facebook', :y=> facebook_count, :color=> 'green'},
              {:name=> 'Twitter', :y=> twitter_count, :color=> 'blue'},
              {:name=> 'Url', :y=> url_count ,:color=> 'yellow'},
              {:name=> 'Rss', :y=> rss_count, :color=> 'red'}
            ],
            :center=> [100, 80], :size=> 100, :showInLegend=> false)

          f.options[:plotOptions][:pie] = {
            :dataLabels => {
              :enabled => true
            }
          }
      end

      ####### Stream Updates über den tag hinweg

      result = Statistics::Dayly.group_by_minute_and_hour "stream_reschedule", Statistics::Dayly.where("value.type" => "reschedule_stream_updates")
      global_streams_minutely = result[:minutely].map do |result|
        result["global_streams_count"]
      end
      global_streams_hourly = result[:hourly].map do |result|
        result["global_streams_count"]
      end

      private_streams_minutely = result[:minutely].map do |result|
        result["private_streams_count"]
      end
      private_streams_hourly = result[:hourly].map do |result|
        result["private_streams_count"]
      end

      @chart_reschedules = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Stream updates, die durch Interval Checker bzw. Logins verursacht werden"})
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:type] = "datetime"
        f.options[:xAxis][:dateTimeLabelFormats] = {
          :day => '%H:%M',
        }
        f.options[:plotOptions] = {
          :series => {
            :marker => {
              radius: 1
            }
          }
        }
        f.options[:yAxis] = [{
            title: {
              text: 'Streams pro Minute'
            },
            min: 0
          },
          {
            title: {
              text: 'Streams pro Stunde'
            },
            min: 0
          }]

        f.series(:type=> 'line',:name=> 'Globale Streams pro Minute',
          :data => global_streams_minutely, :pointStart => Date.today,
          :pointInterval => 1.day/1440, :yAxis => 0,
          :lineWidth => 1, :color => "#005fad")
        f.series(:type=> 'spline',:name=> 'Globale Streams pro Stunde',
          :data => global_streams_hourly, :pointStart => Date.today,
          :pointInterval => 1.day/24, :yAxis => 1,
          :lineWidth => 2, :color => "#006eff")


        f.series(:type=> 'line',:name=> 'Private Streams pro Minute', :data => private_streams_minutely,
         :pointStart => Date.today, :pointInterval => 1.day/1440, :yAxis => 0,
         :lineWidth => 1, :color => "#d34c5b")
        f.series(:type=> 'spline',:name=> 'Private Streams pro Stunde', :data => private_streams_hourly,
          :pointStart => Date.today, :pointInterval => 1.day/24,
          :yAxis => 1, :lineWidth => 2, :color => "#e21444")
      end

      ####### Stream Updates users VS rank worker activity

      result = Statistics::Dayly.where("value.type" => "user_vs_rank_worker").asc("_id")
      uid_counts = []
      interval_stream_schedules_counts = []
      article_counts = []
      stream_counts = []
      abort_rates = []
      shortcut_rates = []
      timestamps = []
      result.each do |result|
        uid_counts << result["value"]["uids"].uniq.count
        interval_stream_schedules_counts << result["value"]["interval_streams"]
        article_counts << result["value"]["article_count"]
        stream_counts << result["value"]["streams"]
        abort_rates << result["value"]["abort_rate"]
        shortcut_rates << result["value"]["shortcut_rate"]
        timestamps << result["_id"]
      end

      @chart_reschedules_vs_rank = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Zusammenhang zwischen Stream updates durch Logins oder den Interval Checker und dem Throughput durch die Pipeline"})
        f.options[:chart][:zoomType] = "x"
        f.options[:chart][:height] = "500"
        f.options[:xAxis][:type] = "datetime"
        f.options[:xAxis][:categories] = timestamps
        f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right' }
        f.options[:xAxis][:tickInterval] = 10
        f.options[:plotOptions] = {
          :series => {
            :marker => {
              radius: 2
            }
          },
          area: {
            stacking: 'normal',
            lineColor: '#666666',
            lineWidth: 1,
            marker: {
              enabled: false
            }
          }
        }

        f.options[:yAxis] = [{
            title: {
              text: 'Anzahl'
            },
            min: 0
          },
          {
            title: {
              text: 'Anzahl Artikel'
            },
            min: 0
          }]

        f.series(:type=> 'line',:name=> 'Anzahl zu aktualisierender Streams durch Logins/Aktivität',
          :data => stream_counts, :yAxis => 0, :type => "area", :stack => "schedules",
          :lineWidth => 1, :color => "#0000ff")
        f.series(:type=> 'line',:name=> 'Anzahl zu aktualisierender Streams durch Interval Checker',
          :data => interval_stream_schedules_counts, :yAxis => 0, :type => "area", :stack => "schedules",
          :lineWidth => 1, :color => "#000099")

        f.series(:type=> 'spline',:name=> 'Artikeldurchsatz in Rank Worker',
          :data => article_counts, :yAxis => 1, :type => "area", :stack => "throughput",
          :lineWidth => 1, :color => "#ff0000")
        f.series(:type=> 'spline',:name=> 'Abbruchrate (Verarbeitung von Artikel nicht notwendig)',
          :data => abort_rates, :yAxis => 1, :type => "area", :stack => "throughput",
          :lineWidth => 1, :color => "#cc0000")
        f.series(:type=> 'spline',:name=> 'Abkürzungsrate (nur Update von Artikel notwendig)',
          :data => shortcut_rates, :yAxis => 1, :type => "area", :stack => "throughput",
          :lineWidth => 1, :color => "#990000")

        f.series(:type=> 'line',:name=> 'Anzahl eindeutiger NutzerInnen die Updates verursachen',
          :data => uid_counts, :yAxis => 0,
          :lineWidth => 1, :color => "#00ff00")
      end


      # # ####### histogramm user anzahl an private streams
      # result = Statistics::Dayly.histogramm(Statistics::Dayly.where("value.type" => "user_stats"), "private_streams_count", 3)

      # @user_stats_private_stream_counts = LazyHighCharts::HighChart.new('graph') do |f|
      #   f.title({ :text=>"Histogramm über die Anzahl an privaten Streams pro User"})
      #   f.options[:xAxis][:type] = "linear"
      #   f.options[:chart][:zoomType] = "x"
      #   f.options[:xAxis][:categories] = result[:labels]
      #   f.options[:xAxis][:labels] = { :rotation => -90, :align => 'right' }
      #   f.options[:yAxis] = [{
      #       title: {
      #         text: 'Häufigkeit'
      #       },
      #       min: 0
      #     }]

      #   f.series(:type=> 'column',:name=> 'Häufigkeiten privater Streams',
      #     :data => result[:data], :color => "#005fad", dataLabels: {
      #       enabled: true
      #     })
      # end

      ####### histogramm artikel pro user
      result = Statistics::Dayly.where("value.type" => "stream_lengths_user_count")
      data = result.map do |result|
        [result["value"]["avg_stream_length"], result["value"]["users_subscribed_count"]]
      end

      @histogramm_feed_length_user_count = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"Histogramm Feedlänge-Abonnierte User. Die Feedlänge ist ein durchschnittswert"})
        f.options[:chart] = {
          type: 'scatter',
          zoomType: 'xy'
        }
        f.options[:xAxis][:type] = "linear"
        f.options[:xAxis] = [{
            title: {
              text: 'Feedlänge'
            },
            min: 0,
            startOnTick: true,
            endOnTick: true,
            showLastLabel: true
          }]
        f.options[:yAxis] = [{
            title: {
              text: 'Abonnentenanzahl'
            },
            min: 0
          }]
        f.options[:plotOptions] = {
          scatter: {
            tooltip: {
              headerFormat: '<b>Daten:</b><br>',
              pointFormat: 'Feedlänge: {point.x}, Abonnentenanzahl: {point.y}'
            }
          }
        }

        f.series(:name=> 'foo',
          :data => data, :color => "rgba(223, 83, 83, .5)")
      end

      # histogramm abonnentenanzahl

      gs_stats = {}
      sum = 0
      GlobalStream::Rss.all.each do |s|
        gs_stats[s.user_ids.count] = 0 unless gs_stats[s.user_ids.count]
        gs_stats[s.user_ids.count] += 1
        sum += s.user_ids.count
      end

      gs_stats = gs_stats.sort_by { |key,value| -value }

      @histogramm_abo_count = LazyHighCharts::HighChart.new('graph') do |f|
        f.title({ :text=>"AbonnentInnen Anzahlen pro Stream"})
        f.options[:chart][:zoomType] = "x"
        f.options[:xAxis][:categories] = gs_stats.map do |array| array[0] end
        f.options[:xAxis][:type] = "linear"
        f.options[:xAxis][:title] = { text: "AbonnentInnen Anzahl" }
        f.options[:yAxis] = [{
            title: {
              text: 'Häufigkeit'
            },
            min: 0,
            plotLines:[{
              value: sum/GlobalStream::Rss.count.to_f,
              color: '#ff0000',
              width:2,
              zIndex:4,
              label:{text:"Durchschnitt #{(sum/GlobalStream::Rss.count.to_f).round(2)}"}
            }]
          }]

        f.series(:type=> 'column',:name=> 'Häufigkeit',
          :data => gs_stats.map do |array| array[1] end, :color => "#005fad", dataLabels: {
            enabled: true
          })
      end
    end
  end
end
