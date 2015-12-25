module Sync
  module Clients
    class Stomp
      attr_reader :adapter

      def setup
        require 'stomp'

        @adapter ||= ::Stomp::Client.new(Sync.server)
      end

      def batch_publish(*args)
        Message.batch_publish(@adapter, *args)
      end

      def build_message(*args)
        Message.new(*args)
      end

      # Public: Normalize channel to adapter supported format
      #
      # channel - The string channel name
      #
      # Returns The normalized channel prefixed with supported format for Stomp
      def normalize_channel(channel)
        return "#{Sync.destination}#{channel}" unless Sync.destination[-1] == '/'
        "#{Sync.destination}/#{channel}"
      end


      class Message

        attr_accessor :channel, :client, :data

        def self.batch_publish(client, messages)
          messages.each do |message|
            message.client = client
            message.publish
          end
        end

        def initialize(channel, data)
          self.channel = channel
          self.data = data
        end

        def publish
          if Sync.async?
            publish_asynchronous
          else
            publish_synchronous
          end
        end

        def publish_synchronous
          client.publish(channel, data.to_json)
        end

        def publish_asynchronous
          Sync.reactor.perform do
            client.publish(channel, data.to_json)
          end
        end
      end
    end
  end
end
