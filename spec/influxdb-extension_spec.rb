require "sensu/extensions/influxdb"
require "sensu/logger"

describe "Sensu::Extension::InfluxDB" do

  before do
    @extension = Sensu::Extension::InfluxDB.new
    @extension.settings = Hash.new
    @extension.settings["influxdb-extension"] = {
        :database => "test",
        :hostname => "nonexistinghost",
        :additional_handlers => ["proxy"],
        :buffer_size => 5,
        :buffer_max_age => 1,
        :custom_measurements => [
          {:measurement_name => 'measurement1', :measurement_formats => ['_._.measurement.htype.metric*', '_.measurement.metric*'], :apply_only_for_checks => ['statsd']},
          {:measurement_name => 'measurement2', :measurement_formats => ['_._.measurement.htype.metric*','_.measurement.htype.metric*']},
          {:measurement_name => 'measurement3', :measurement_formats => ['_._.measurement.metric'], :apply_only_for_checks => ['other']},
          {:measurement_name => 'measurement_all', :measurement_formats => ['_._.measurement.metric']},
          {:measurement_name => 'measurement_tag', :measurement_formats => ['_._.measurement.metric']},
        ]
    }
    @extension.settings["proxy"] = {
        :proxy_mode => true
    }
    
    @extension.instance_variable_set("@logger", Sensu::Logger.get(:log_level => :fatal))
    @extension.post_init
  end

  it "processes minimal event" do
    @extension.run(minimal_event.to_json) do 
      buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
      expect(buffer[0]).to eq("check_name rspec=69 1480697845")
    end
  end

  
  it "skips events with invalid timestamp" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "rspec 69 invalid"
      }
    }

    @extension.run(event.to_json) do 
      buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
      expect(buffer.size).to eq(0)
    end
  end

  
  it "flushes buffer when full" do
    5.times {
      @extension.run(minimal_event.to_json) do |output,status|
        expect(output).to eq("ok")
        expect(status).to eq(0)
      end
    }
    # flush buffer will fail writing to bogus influxdb
    2.times {
      @extension.run(minimal_event.to_json) do |output,status|
        expect(output).to eq("error")
        expect(status).to eq(2)
      end
    }
  end

  it "flushes buffer when timed out" do
    @extension.run(minimal_event.to_json) do end
    sleep(1)
    @extension.run(minimal_event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer.size).to eq(1)
  end

  it "sorts event tags alphabetically" do
    event = {
      "client" => {
        "name" => "rspec",
        "tags" => {
          "x" => "1",
          "z" => "1",
          "a" => "1"
        }
      },
      "check" => {
        "name" => "check_name",
        "output" => "rspec 69 1480697845",
        "tags" => {
          "b" => "1",
          "c" => "1",
          "y" => "1"
        }
      }
    }
    
    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,a=1,b=1,c=1,x=1,y=1,z=1 rspec=69 1480697845")
  end

  it "Accepting output formats" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.apache.request 69 1480697845",
        "influxdb" => {"output_formats" => ['host.type.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=apache request=69 1480697845")
  end

  it "Accepting underscore in formats" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.apache.unwanted.request 69 1480697845",
        "influxdb" => {"output_formats" => ['host.type._.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=apache request=69 1480697845")
  end

  it "Accepting fieldset with same tagset" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.apache.unwanted.request 69 1480697845\nhost_name.apache.unwanted.errors 1 1480697845",
        "influxdb" => {"output_formats" => ['host.type._.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=apache request=69,errors=1 1480697845")
  end

  it "Accepting fieldset with different tagset" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.server1.unwanted.request 69 1480697845\nhost_name.server2.unwanted.errors 1 1480697845",
        "influxdb" => {"output_formats" => ['host.type._.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=server1 request=69 1480697845")
    expect(buffer[1]).to eq("check_name,host=host_name,type=server2 errors=1 1480697845")
  end

  it "ignoring fields different tags" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.server1.unwanted.request 69 1480697845\nhost_name.server1.unwanted.timeout 0 1480697845\nhost_name.server1.unwanted.request1 69 1480697845\nhost_name.server2.unwanted.errors 1 1480697845",
        "influxdb" => {"output_formats" => ['host.type._.metric'], "ignore_fields" => ['request', 'request1']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=server1 timeout=0 1480697845")
    expect(buffer[1]).to eq("check_name,host=host_name,type=server2 errors=1 1480697845")
  end

  it "ignoring fields with same tags" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.server1.unwanted.request 69 1480697845\nhost_name.server1.unwanted.timeout 0 1480697845\nhost_name.server1.unwanted.request1 69 1480697845\nhost_name.server1.unwanted.errors 1 1480697845",
        "influxdb" => {"output_formats" => ['host.type._.metric'], "ignore_fields" => ['request', 'request1']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=server1 timeout=0,errors=1 1480697845")
    expect(buffer[1]).to eq(nil)
  end

  it "Accepting metric*" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.server1.unwanted.request 69 1480697845",
        "influxdb" => {"output_formats" => ['host.metric*']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name server1.unwanted.request=69 1480697845")
    expect(buffer[1]).to eq(nil)
  end

  it "Accepting metric* with underscore _" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.server1.unwanted.request 69 1480697845",
        "influxdb" => {"output_formats" => ['_.metric*']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name server1.unwanted.request=69 1480697845")
    expect(buffer[1]).to eq(nil)
  end

  it "Accepting metric* with same tagset" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.apache.unwanted.request 69 1480697845\nhost_name.apache.unwanted.errors 1 1480697845",
        "influxdb" => {"output_formats" => ['host.type.metric*']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=apache unwanted.request=69,unwanted.errors=1 1480697845")
    expect(buffer[1]).to eq(nil)
  end

  it "Accepting metric* with different tagset" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.server1.unwanted.request 69 1480697845\nhost_name.server1.unwanted.timeout 0 1480697845\nhost_name.server1.unwanted.request1 69 1480697845\nhost_name.server2.unwanted.errors 1 1480697845",
        "influxdb" => {"output_formats" => ['host.type.metric*']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=server1 unwanted.request=69,unwanted.timeout=0,unwanted.request1=69 1480697845")
    expect(buffer[1]).to eq("check_name,host=host_name,type=server2 unwanted.errors=1 1480697845")
    expect(buffer[2]).to eq(nil)
  end

  it "Accepting metgric*, ignoring fields different tags" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.server1.unwanted.request 69 1480697845\nhost_name.server1.unwanted.timeout 0 1480697845\nhost_name.server1.unwanted.request1 69 1480697845\nhost_name.server2.unwanted.errors 1 1480697845",
        "influxdb" => {"output_formats" => ['host.type.metric*'], "ignore_fields" => ['unwanted.request', 'unwanted.request1']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=server1 unwanted.timeout=0 1480697845")
    expect(buffer[1]).to eq("check_name,host=host_name,type=server2 unwanted.errors=1 1480697845")
  end

  it "Accepting metric*, ignoring fields with same tags" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.server1.unwanted.request 69 1480697845\nhost_name.server1.unwanted.timeout 0 1480697845\nhost_name.server1.unwanted.request1 69 1480697845\nhost_name.server1.unwanted.errors 1 1480697845",
        "influxdb" => {"output_formats" => ['host.type.metric*'], "ignore_fields" => ['unwanted.request', 'unwanted.request1']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=server1 unwanted.timeout=0,unwanted.errors=1 1480697845")
    expect(buffer[1]).to eq(nil)
  end

  it "Accepting metric*, in order, metric as priority" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.server1.unwanted.request 69 1480697845\nhost_name.server1.unwanted.timeout 0 1480697845\nhost_name.server1.unwanted.request1 69 1480697845\nhost_name.server1.unwanted.errors 1 1480697845",
        "influxdb" => {"output_formats" => ['_._._.metric', '_.metric*']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name request=69,timeout=0,request1=69,errors=1 1480697845")
    expect(buffer[1]).to eq(nil)
  end

  it "Accepting metric*, in order, metric* as priority" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.server1.unwanted.request 69 1480697845\nhost_name.server1.unwanted.timeout 0 1480697845\nhost_name.server1.unwanted.request1 69 1480697845\nhost_name.server1.unwanted.errors 1 1480697845",
        "influxdb" => {"output_formats" => ['_.metric*','_._._.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name server1.unwanted.request=69,server1.unwanted.timeout=0,server1.unwanted.request1=69,server1.unwanted.errors=1 1480697845")
    expect(buffer[1]).to eq(nil)
  end

  it "Accepting fieldset with different timestamp as different entry" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.apache.unwanted.request 69 1480697845\nhost_name.apache.unwanted.errors 1 1480697846",
        "influxdb" => {"output_formats" => ['host.type._.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=apache request=69 1480697845")
    expect(buffer[1]).to eq("check_name,host=host_name,type=apache errors=1 1480697846")
  end

  it "Accepting field in between" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.apache.unwanted.request.x 69 1480697845\nhost_name.apache.unwanted.errors 1 1480697846",
        "influxdb" => {"output_formats" => ['host.type._.metric._']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=apache request=69 1480697845")
  end

  it "Accepting string values" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.apache.version 1.0.2 1480697845\nhost_name.apache.license MIT 1480697845\nhost_name.apache.contrib \"X, Y\" 1480697845",
        "influxdb" => {"output_formats" => ['host.type.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=apache version=\"1.0.2\",license=\"MIT\",contrib=\"X, Y\" 1480697845")
  end

  it "Accepting explicit string values" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "host_name.apache.version 1.0 1480697845\nhost_name.apache.subver 2.3 1480697845\n",
        "influxdb" => {"output_formats" => ['host.type.metric'], "string_fields" => ["version"] }
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("check_name,host=host_name,type=apache version=\"1.0\",subver=2.3 1480697845")
  end

  it "Ensure portability for ouput formats in influxdb" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "statsd",
        "output" => "statsd.timers.env1.a.b.c 1.0 1480697845\nstatsd.timers.env1.p.q 10.0 1480697845\nstatsd.gauges.env1.a 2.3 1480697845\n",
        "influxdb" => {"output_formats" => ['_._.type.metric*']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("statsd,type=env1 a.b.c=1.0,p.q=10.0,a=2.3 1480697845")
  end

  it "Measurment based on prefix configured" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "statsd",
        "output" => "statsd.timers.measurement1.env1.a.b.c 1.0 1480697845\nstatsd.timers.measurement1.env1.a.b.c.d 2.0 1480697845\nstatsd.measurement1.p.q 10.0 1480697845\nstatsd.gauges.measurement2.env1.a 2.3 1480697845\nstatsd.measurement2.env1.b 2.3 1480697845\nstatsd.others.env1.a 1.0 1480697845\n",
        "influxdb" => {"output_formats" => ['_._.type.metric*']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("measurement1,htype=env1 a.b.c=1.0,a.b.c.d=2.0 1480697845")
    expect(buffer[1]).to eq("measurement1 p.q=10.0 1480697845")
    expect(buffer[2]).to eq("measurement2,htype=env1 a=2.3,b=2.3 1480697845")
    expect(buffer[3]).to eq("statsd,type=env1 a=1.0 1480697845")
  end

  it "Ensure Measurement overrides default formats" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "statsd",
        "output" => "statsd.timers.measurement_tag.metric1 1.0 1480697845",
        "influxdb" => {"output_formats" => ['_._.type.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("measurement_tag metric1=1.0 1480697845")
  end

  it "Ensure Measurement to default formats if doesnt match in measurement priority" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "statsd",
        "output" => "statsd.timers.tag2.metric1 1.0 1480697845",
        "influxdb" => {"output_formats" => ['_._.type.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("statsd,type=tag2 metric1=1.0 1480697845")
  end

  it "Ensure Measurement filter applied to checks" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "other",
        "output" => "statsd.timers.tag2.metric1 1.0 1480697845\nstatsd.timers.measurement3.metric1 1.0 1480697845",
        "influxdb" => {"output_formats" => ['_._.type.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("other,type=tag2 metric1=1.0 1480697845")
    expect(buffer[1]).to eq("measurement3 metric1=1.0 1480697845")
  end

  it "Ensure Measurement filter not applied to other checks" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "other1",
        "output" => "statsd.timers.tag2.metric1 1.0 1480697845\nstatsd.timers.measurement3.metric1 1.0 1480697845",
        "influxdb" => {"output_formats" => ['_._.type.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("other1,type=tag2 metric1=1.0 1480697845")
    expect(buffer[1]).to eq("other1,type=measurement3 metric1=1.0 1480697845")
  end

  it "Ensure Measurement filter applied to all checks if not specified" do
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "random",
        "output" => "statsd.timers.measurement_all.metric1 1.0 1480697845",
        "influxdb" => {"output_formats" => ['_._.type.metric']}
      }
    }

    @extension.run(event.to_json) do end

    buffer = @extension.instance_variable_get("@handlers")["influxdb-extension"]["buffer"]
    expect(buffer[0]).to eq("measurement_all metric1=1.0 1480697845")
  end

end

def minimal_event
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "name" => "check_name",
        "output" => "rspec 69 1480697845"
      }
    }
end

def minimal_event_proxy
    event = {
      "client" => {
        "name" => "rspec"
      },
      "check" => {
        "check_name" => "check_name",
        "handlers" => ["proxy"],
        "output" => "rspec 69 1480697845"
      }
    }
end
