require 'lims-messagebusclient'

amqp_settings = YAML.load_file(File.join('config','amqp.yml'))["development"] 
mysql_settings = YAML.load_file(File.join('config','database.yml'))["development"] 

creator = Lims::MessageBusClient::SequencescapePlateCreator.new("sequencescape_plate_creator", amqp_settings, mysql_settings)
puts "Creator starts..."
creator.start
