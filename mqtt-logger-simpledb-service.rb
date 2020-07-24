
# SAMPLE:
# dir = "./data"
# #result = read_data(ARGV[0], {:count => 3, :since => 1595579754, :dir => dir})
# #result = read_data(ARGV[0], {:max_time => 1595580054, :dir => dir})
# #result = read_data(ARGV[0], {:count => 3, :dir => dir})
# result = read_data(ARGV[0], {:max_time => 1595580154, :dir => dir})

#
# USAGE:
#   rackup
#

require 'grape'
require 'json'
require './mqtt-logger-simpledb-functions.rb'

class MQTT_SimpleDB < Grape::API
  format :json
  get '/mqttdata' do
     dir = "./data"
     options = {:dir => dir, :count => params["count"],
        :since_time => params["since_time"],
        :max_time => params["max_time"]}
     results = read_jsondata(params["topic"], options)
     
     result = { 'results': results }
     return (result)
  end
end

