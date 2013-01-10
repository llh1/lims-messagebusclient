require 'lims-messagebusclient/consumer'

module Lims
  module MessageBusClient
    class Auditor
      include Consumer
      attribute :audit_file, String, :required => true, :writer => :private

      # Initialize the auditor
      # @param [Hash] settings for amqp connection
      # @param [String] audit file path
      def initialize(amqp_settings, audit_file)
        setup(amqp_settings)
        @audit_file = audit_file 
        set_auditor_queue
      end

      # Set the auditor queue with a routing key # 
      # which means it's gonna catch all the messages
      # on the bus. 
      def set_auditor_queue
        self.add_queue("audit", "#") do |metadata, payload|
          metadata.ack
          write_message(metadata, payload)
        end
      end
      private :set_auditor_queue   

      # Write the message metadata and payload
      # in a file. Append the message to the end
      # of the file.
      def write_message(metadata, payload)
        File.open(audit_file, "a") do |f|
          f.puts("routing key = #{metadata.routing_key}")
          f.puts("content-type = #{metadata.content_type}")
          f.puts(payload)
          f.puts
        end
      end
      private :write_message

    end
  end
end
