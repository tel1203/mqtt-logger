require "mqtt"
require "fileutils"
require "erb"
require './mqtt-logger-simpledb-functions.rb'

mqtt_host = "192.168.11.151"
mqtt_port = "1883"
flag_ssl = false

dir = "./data"

if (flag_ssl) then
  cert_file = "cert.pem"
  key_file = "thing-private-key.pem",
  ca_file = "rootCA.pem"

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

while (true) do
  client.subscribe("#")
  topic,message = client.get
  p [topic, message]

  output_dir, output_file = make_dirfilename(topic, dir)
  output = sprintf("%s\t%s\t%s", Time.now.to_i, topic, ERB::Util.url_encode(message))

  FileUtils.mkdir_p(output_dir)
  f = open(output_file, "a")
  f.puts(output)
  f.close()
end

