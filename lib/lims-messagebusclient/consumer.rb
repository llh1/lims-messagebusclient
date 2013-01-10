require 'common'

module Lims
  module MessageBusClient
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
          attribute :host, String, :required => true, :writer => :private
          attribute :port, String, :required => true, :writer => :private
          attribute :user, String, :required => false, :writer => :private
          attribute :password, String, :required => false, :writer => :private
          attribute :exchange_name, String, :required => true, :writer => :private
          attribute :durable, String, :required => true, :writer => :private
        end
      end

      # Setup the consumer with amqp settings
      # @param [Hash] settings
      def setup(settings = {})
        @host = settings["host"]
        @port = settings["port"]
        @user = settings["user"]
        @password = settings["password"]
        @exchange_name = settings["exchange_name"]
        @durable = settings["durable"] 
        @queues = {} 
      end

      # Register a new queue 
      # All the messages published which match the routing key
      # will be available in the queue and processed by the
      # queue handler.
      # @param [String] queue name
      # @param [String] routing key
      # @param [Block] queue handler
      def add_queue(queue_name, routing_key, &queue_handler)
        @queues[queue_name] = {:routing_key => routing_key, :queue_handler => queue_handler} 
      end

      # Start the consumer
      # Connect to RabbitMQ, create/use a topic exchange and
      # setup the queues.
      def start
        raise InvalidSettingsError, "settings are invalid" unless valid?

        connection_settings = {:host => host, 
                               :port => port,
                               :on_tcp_connection_failure => connection_failure_handler,
                               :on_possible_authentication_failure => authentication_failure_handler}

        AMQP::start(connection_settings) do |connection|
          channel = AMQP::Channel.new(connection)
          exchange = AMQP::Exchange.new(channel, :topic, exchange_name, :durable => durable)

          @queues.each do |queue_name, settings|
            queue = channel.queue(queue_name, :durable => durable)
            queue.bind(exchange, :routing_key => settings[:routing_key])
            queue.subscribe(:ack => true, &settings[:queue_handler])
          end
        end
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

      private :connection_failure_handler, :authentication_failure_handler
    end
  end
end
