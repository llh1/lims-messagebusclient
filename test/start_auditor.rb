require 'lims-messagebusclient'

amqp_settings = YAML.load_file(File.join('config','amqp.yml'))["development"] 
audit_settings = YAML.load_file(File.join('config','audit.yml'))["development"] 

auditor = Lims::MessageBusClient::Auditor.new("audit", amqp_settings, audit_settings["audit_file"])
puts "Auditor starts..."
puts "Incoming messages are stored in #{audit_settings["audit_file"]}" 
puts

begin
  auditor.start
rescue Exception => e
  puts "Something went wrong..."
  puts e.inspect
end
