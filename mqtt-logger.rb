require "mqtt"
require "fileutils"
require 'uri'

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

  tmp = topic.split("/")
  data_dir = tmp[0, tmp.size-1]
  data_file = tmp[-1]

  output_dir  = dir+"/"+data_dir.join("/")
  output_file = dir+"/"+data_dir.join("/")+"/"+data_file+".txt"
  output = sprintf("%s\t%s\t%s", Time.now.to_i, topic, URI.escape(message))

  FileUtils.mkdir_p(output_dir)
  f = open(output_file, "a")
  f.puts(output)
  f.close()
end

