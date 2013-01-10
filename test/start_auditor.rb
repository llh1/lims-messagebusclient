require 'lims-messagebusclient'

amqp_settings = YAML.load_file(File.join('config','amqp.yml'))["development"] 
audit_settings = YAML.load_file(File.join('config','audit.yml'))["development"] 

auditor = Lims::MessageBusClient::Auditor.new(amqp_settings, audit_settings["audit_file"])
auditor.start
