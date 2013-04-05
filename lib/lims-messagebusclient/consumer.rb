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

      RECONNECT_INTERVAL = 30

      # Define the parameters needed to connect to RabbitMQ
      # and to setup the exchange.
      def self.included(klass)
        klass.class_eval do
          include Virtus
          include Aequitas
          attribute :url, String, :required => true, :writer => :private
          attribute :exchange_name, String, :required => true, :writer => :private
          attribute :durable, String, :required => true, :writer => :private
          attribute :empty_queue_disconnect_interval, Numeric, :required => false, :writer => :private
        end
      end

      # Setup the consumer with amqp settings
      # @param [Hash] settings
      def consumer_setup(settings = {})
        @url = settings["url"]
        @exchange_name = settings["exchange_name"]
        @durable = settings["durable"] 
        @empty_queue_disconnect_interval = settings["empty_queue_disconnect_interval"] || 0
        @queues = {} 
      end

      # Register a new queue 
      # All the messages published which match the routing key
      # will be available in the queue and processed by the
      # queue handler.
      # @param [String] queue name
      # @param [Array<String>] routing keys
      # @param [Block] queue handler
      def add_queue(queue_name, routing_keys = [], &queue_handler)
        routing_keys.each do |routing_key|
          raise InvalidSettingsError, "routing keys are not in the right format" unless RoutingKey.valid?(routing_key)
        end
        @queues[queue_name] = {:routing_keys => routing_keys, :queue_handler => queue_handler}
      end

      # Start the consumer
      # Connect to RabbitMQ, create/use a topic exchange and
      # setup the queues.
      def start
        raise InvalidSettingsError, "settings are invalid" unless valid?

        AMQP::start(connection_settings) do |connection|
          setup_manual_termination(connection)
          setup_reconnection(connection)
          build(connection)
        end
      end

      private

      # Build the consumer
      # @param [AMQP::Session] connection
      def build(connection)
        channel = AMQP::Channel.new(connection)
        exchange = AMQP::Exchange.new(channel, :topic, exchange_name, :durable => durable)

        @queues.each do |queue_name, settings|
          queue = channel.queue(queue_name, :durable => durable)
          setup_automatic_termination(connection, queue)
          settings[:routing_keys].each do |routing_key|
            queue.bind(exchange, :routing_key => routing_key)
          end
          queue.subscribe(:ack => true, &settings[:queue_handler])
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

      # Handler executed after a connection loss. 
      # Try periodically to reconnect and setup the connection.
      def setup_reconnection(connection)
        connection.on_error do |conn, connection_close|
          if connection_close.reply_code == 320
            connection.periodically_reconnect(RECONNECT_INTERVAL)
          end
        end

        connection.on_recovery do
          build(connection)          
        end
      end

      # Handler for terminate the consumer
      def setup_manual_termination(connection)
        ['TERM', 'INT'].each do |signal|
          Signal.trap(signal) do
            connection.close { EventMachine.stop }
          end
        end
      end

      # Terminate the consumer after a time defined in
      # empty_queue_disconnect_interval variable. 
      # If 0, it doesn't terminate if there is no 
      # message in the queue.
      def setup_automatic_termination(connection, queue)
        EventMachine.add_periodic_timer(empty_queue_disconnect_interval) do
          queue.status do |number_of_messages, _|
            if number_of_messages.zero?
              connection.close { EventMachine.stop }
            end
          end
        end unless empty_queue_disconnect_interval.zero?
      end

      module RoutingKey
        ValidationRegex = /^(\w+|\*|#)(\.(\w+|\*|#))*$/

        def self.valid?(routing_key)
          ValidationRegex.match(routing_key)
        end
      end
    end
  end
end
