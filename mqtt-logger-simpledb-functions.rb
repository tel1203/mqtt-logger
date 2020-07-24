require "mqtt"
require "json"
require 'cgi'

def make_dirfilename(topic, dir="")
  tmp = topic.split("/")
  data_dir = tmp[0, tmp.size-1]
  data_file = tmp[-1]

  output_dir  = dir+"/"+data_dir.join("/")
  output_file = dir+"/"+data_dir.join("/")+"/"+data_file+".txt"

  return ([output_dir, output_file])
end

def read_jsondata(topic, options={})
  dir = options[:dir]?options[:dir]:""
  count = (options[:count]!=nil)?options[:count]:0
  since_time = options[:since_time]?options[:since_time]:0
  max_time = options[:max_time]?options[:max_time]:0
  count = count.to_i
  since_time = since_time.to_i
  max_time = max_time.to_i

  output_dir, output_file = make_dirfilename(topic, dir)
  result = Hash.new
  result["results"] = Array.new

  begin
    i = 1
    File.foreach(output_file).reverse_each { |line|
      tmp1, tmp2, tmp3 = line.chop.split("\t")
      time = tmp1.to_i
      topic = tmp2
      message = CGI.unescape(tmp3 || "")
      next if (time >= max_time && max_time != 0)
      break if (time < since_time)
  
      record = {:time => time, :topic => topic, :message => message}
      result["results"].unshift(record)
  
      break if (i >= count && count != 0)
      i += 1
    }
  rescue Errno::ENOENT
  rescue
  end
  
   return (result.to_json)
end

