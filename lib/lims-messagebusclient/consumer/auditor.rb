require 'lims-messagebusclient/consumer'

module Lims
  module MessageBusClient
    # The auditor consumer reads all the messages on the
    # bus. As a result, it defines a single queue with a
    # routing key '#' which match all the messages available.
    # The auditor stores all the messages in a file.
    class Auditor
      include Consumer
      attribute :auditor_name, String, :required => true, :writer => :private
      attribute :audit_file, String, :required => true, :writer => :private

      # Initialize the auditor
      # @param [Hash] settings for amqp connection
      # @param [String] audit file path
      def initialize(auditor_name, amqp_settings, audit_file)
        setup(amqp_settings)
        @auditor_name = auditor_name
        @audit_file = audit_file 
        set_auditor_queue
      end

      # Set the auditor queue with a routing key # 
      # which means it's gonna catch all the messages
      # on the bus. 
      def set_auditor_queue
        self.add_queue(auditor_name, ["#"]) do |metadata, payload|
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
        end
      end
      private :write_message

    end
  end
end
