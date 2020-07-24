require './mqtt-logger-simpledb-service.rb'
run Rack::Cascade.new [MQTT_SimpleDB]

