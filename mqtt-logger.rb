require "mqtt"
require "fileutils"
require "erb"
require "yaml"
require 'optparse'
require './mqtt-logger-simpledb-functions.rb'

config_file = nil
opt = OptionParser.new
opt.on('-c', '--config FILE', 'config file') { |v| config_file = v }
opt.parse(ARGV)

config_file = "config.yml" if (config_file == nil)
printf("Read Config: [%s]\n", config_file)

config = YAML.load_file(config_file)
p config

def set_client(mqtt_host, mqtt_port,
  flag_ssl=false, cert_file="", key_file="", ca_file="")

  client = nil
  if (flag_ssl) then
    printf("Connecct MQTT-SSL [%s:%s]\n",
      mqtt_host, mqtt_port)
    client = MQTT::Client.connect(
        host: mqtt_host,
        port: mqtt_port,
        ssl: true,
        cert_file: cert_file,
        key_file: key_file,
        ca_file: ca_file)
  else
    printf("Connecct MQTT [%s:%s]\n",
      mqtt_host, mqtt_port)
    client = MQTT::Client.connect(
        host: mqtt_host,
        port: mqtt_port)
  end

  return (client) 
end

client = set_client(
  config["mqtt_host"],
  config["mqtt_port"],
  config["flag_ssl"],
  config["cert_file"],
  config["key_file"],
  config["ca_file"]
)

dir = config["data_dir"]
while (true) do

  begin
    client.subscribe("#")
    topic,message = client.get
  rescue => error
    p error
    client = set_client(
      config["mqtt_host"],
      config["mqtt_port"],
      config["flag_ssl"],
      config["cert_file"],
      config["key_file"],
      config["ca_file"]
    )
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

