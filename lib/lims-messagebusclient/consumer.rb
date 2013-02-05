require 'common'

module Lims
  module MessageBusClient
    # A consumer connects to a RabbitMQ broker and receives
    # messages in its queues. A consumer can deal with several
    # queues if necessary.
    module Consumer

      # Exception raised if settings are invalid
      class InvalidSettingsError < StandardError
      end

      # Exception raised after a failed connection to RabbitMQ
      class ConnectionError < StandardError
      end

      # Exception raised after an authentication failure to RabbitMQ
      class AuthenticationError < StandardError
      end

      # Define the parameters needed to connect to RabbitMQ
      # and to setup the exchange.
      def self.included(klass)
        klass.class_eval do
          include Virtus
          include Aequitas
          attribute :url, String, :required => true, :writer => :private
          attribute :exchange_name, String, :required => true, :writer => :private
          attribute :durable, String, :required => true, :writer => :private
        end
      end

      # Setup the consumer with amqp settings
      # @param [Hash] settings
      def consumer_setup(settings = {})
        @url = settings["url"]
        @exchange_name = settings["exchange_name"]
        @durable = settings["durable"] 
        @queues = {} 
      end

      # Register a new queue 
      # All the messages published which match the routing key
      # will be available in the queue and processed by the
      # queue handler.
      # @param [String] queue name
      # @param [Array<String>] routing keys
      # @param [Block] queue handler
      def add_queue(queue_name, routing_keys, &queue_handler)
        @queues[queue_name] = {:routing_keys => routing_keys, :queue_handler => queue_handler}
      end

      # Start the consumer
      # Connect to RabbitMQ, create/use a topic exchange and
      # setup the queues.
      def start
        raise InvalidSettingsError, "settings are invalid" unless valid?

        AMQP::start(connection_settings) do |connection|
          channel = AMQP::Channel.new(connection)
          exchange = AMQP::Exchange.new(channel, :topic, exchange_name, :durable => durable)

          @queues.each do |queue_name, settings|
            queue = channel.queue(queue_name, :durable => durable)
            settings[:routing_keys].each do |routing_key|
              queue.bind(exchange, :routing_key => routing_key)
            end
            queue.subscribe(:ack => true, &settings[:queue_handler])
          end
        end
      end

      # Build the connection settings hash
      def connection_settings
        connection_settings = AMQP::Client.parse_connection_uri(url)
        connection_settings[:on_tcp_connection_failure] = connection_failure_handler
        connection_settings[:on_possible_authentication_failure] = authentication_failure_handler
        connection_settings
      end

      # Handler executed if a connection to RabbitMQ fails
      def connection_failure_handler
        Proc.new do |settings|
          EventMachine.stop if EventMachine.reactor_running?
          raise ConnectionError, "can't connect to RabbitMQ"
        end
      end

      # Handler executed if an authentication to RabbitMQ fails
      def authentication_failure_handler
        Proc.new do |settings|
          EventMachine.stop if EventMachine.reactor_running?
          raise AuthenticationError, "can't authenticate to RabbitMQ"
        end
      end

      private :connection_settings, :connection_failure_handler, :authentication_failure_handler
    end
  end
end
