require "mqtt"
require "fileutils"
require "erb"
require './mqtt-logger-simpledb-functions.rb'

mqtt_host = "192.168.11.151"
mqtt_port = "1883"
flag_ssl = false

dir = "./data"

def set_client(mqtt_host, mqtt_port,
  flag_ssl=false, cert_file="", key_file="", ca_file="")

  client = nil
  if (flag_ssl) then
#    cert_file = "cert.pem"
#    key_file = "thing-private-key.pem",
#    ca_file = "rootCA.pem"
    client = MQTT::Client.connect(host: mqtt_host,
                         port: mqtt_port,
                         ssl: true,
                         cert_file: cert_file,
                         key_file: key_file,
                         ca_file: ca_file)
  else
    client = MQTT::Client.connect(host: mqtt_host,
                         port: mqtt_port)
  end

  return (client) 
end

client = set_client(mqtt_host, mqtt_port)

while (true) do

  begin
    client.subscribe("#")
    topic,message = client.get
  rescue => error
    p error
    client = set_client(mqtt_host, mqtt_port)
    retry
  end

  output_dir, output_file = make_dirfilename(topic, dir)
  output = sprintf("%s\t%s\t%s", Time.now.to_i, topic, ERB::Util.url_encode(message))
  p output

  FileUtils.mkdir_p(output_dir)
  f = open(output_file, "a")
  f.puts(output)
  f.close()
end

#
#Traceback (most recent call last):
#        3: from /Users/tel/.rbenv/versions/2.7.1/lib/ruby/gems/2.7.0/gems/mqtt-0.5.0/lib/mqtt/client.rb:302:in `block in connect'
#        2: from /Users/tel/.rbenv/versions/2.7.1/lib/ruby/gems/2.7.0/gems/mqtt-0.5.0/lib/mqtt/client.rb:481:in `receive_packet'
#        1: from /Users/tel/.rbenv/versions/2.7.1/lib/ruby/gems/2.7.0/gems/mqtt-0.5.0/lib/mqtt/packet.rb:31:in `read'
#/Users/tel/.rbenv/versions/2.7.1/lib/ruby/gems/2.7.0/gems/mqtt-0.5.0/lib/mqtt/packet.rb:283:in `read_byte': Failed to read byte from socket (MQTT::ProtocolException)
#
