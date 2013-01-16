require 'json'
require 'sequel'
require 'lims-messagebusclient/consumer'

module Lims
  module MessageBusClient
    # When a stock plate is created on S2, it must be created in
    # Sequencescape as well. To identify a stock plate creation in S2,
    # two kinds of message need to be recorded on the message bus:
    # plate creation messages and order creation/update messages.
    # Plate creation messages contain the structure of the plate, whereas 
    # order messages identify the type of the plate in its items. 
    # Every time a plate creation message is received, a new plate is 
    # created in Sequencescape side with a plate purpose set to Unassigned.
    # As soon as we receive an order message which references a plate which 
    # has been created in SS, its plate purpose is updated to Stock Plate.
    # If a stock plate appears in an order message but cannot be found in 
    # Sequencescape database, it means the order message has been received 
    # before the plate creation message. The order message is then requeued 
    # waiting for the plate message to arrive.
    class SequencescapePlateCreator
      include Consumer
      attribute :queue_name, String, :required => true, :writer => :private, :reader => :private
      attribute :mysql_settings, Hash, :required => true, :writer => :private, :reader => :private

      WELL = "Well"
      PLATE = "Plate"
      ASSET = "Asset"
      STOCK_PLATE_PURPOSE_ID = 2
      UNASSIGNED_PLATE_PURPOSE_ID = 100
      STOCK_PLATE = "WGS Stock Plate"
      ITEM_DONE_STATUS = "done"

      # Exception raised after an unsuccessful lookup for a plate 
      # in Sequencescape database.
      class PlateNotFoundInSequencescape < StandardError
      end

      # Initilize the SequencescapePlateCreator class
      # @param [String] queue name
      # @param [Hash] AMQP settings
      # @param [Hash] MySQL settings
      def initialize(queue_name, amqp_settings, mysql_settings)
        setup(amqp_settings)
        @queue_name = queue_name
        @mysql_settings = mysql_settings
        @db = Sequel.connect(:adapter => mysql_settings['adapter'],
                             :host => mysql_settings['host'],
                             :user => mysql_settings['user'],
                             :password => mysql_settings['password'],
                             :database => mysql_settings['database'])
        set_queue
      end

      private

      # Define the routing keys we are interested in.
      # We need messages when a plate is created and when order is
      # created or updated.
      def routing_keys
        ["*.*.plate.create",
         "*.*.order.create",
         "*.*.order.updateorder",
         "*.*.platetransfer.platetransfer"]
      end

      # Setup the queue.
      # Two different behaviour depending on the routing key
      # of the message (plate/order). 
      def set_queue
        self.add_queue(queue_name, routing_keys) do |metadata, payload|
          # On reception of a plate creation message
          if metadata.routing_key =~ /plate\.create/
            plate_message_handler(metadata, payload)       
          # On reception of an order creation/update message
          elsif metadata.routing_key =~ /order\.create|updateorder/
            order_message_handler(metadata, payload)
          # On reception of a plate transfer message
          elsif metadata.routing_key =~ /platetransfer/
            platetransfer_message_handler(metadata, payload)
          end
        end
      end

      # When a plate creation message is received, 
      # the plate is created in Sequencescape database.
      # @param [AMQP::Header] metadata
      # @param [String] plate json
      def plate_message_handler(metadata, plate_json)
        begin 
          create_plate_in_sequencescape(plate_json)
        rescue Sequel::Rollback => e
          metadata.reject(:requeue => true)
          puts "Error saving plate in Sequencescape: #{e}"
        else
          metadata.ack
        end
      end

      # When an order message is received,
      # we check if it contains an item which is a stock plate 
      # with a done status. Otherwise, we just ignore the message
      # and delete the plates which could have been saved in sequencescape
      # but aren't stock plate.
      # We try to update the stock plate on Sequencescape, if the plate
      # is not found in Sequencescape, the message is requeued.
      # @param [AMQP::Header] metadata
      # @param [String] order json
      def order_message_handler(metadata, order_json)
        order = JSON.parse(order_json)["order"]  
        stock_plate = order["items"][STOCK_PLATE]
        other_plates = order["items"].keep_if {|k,v| k != STOCK_PLATE}
        delete_unassigned_plates_in_sequencescape(other_plates)

        if stock_plate && stock_plate["status"] == ITEM_DONE_STATUS 
          begin
            update_plate_purpose_in_sequencescape(stock_plate["uuid"])
          rescue PlateNotFoundInSequencescape => e
            metadata.reject(:requeue => true)
          rescue Sequel::Rollback => e
            metadata.reject(:requeue => true)
            puts "Error updating plate in Sequencescape: #{e}"
          else
            metadata.ack
          end
        else
          metadata.ack
        end 
      end

      def platetransfer_message_handler(metadata, platetransfer_json)
        platetransfer = JSON.parse(platetransfer_json)["plate_transfer"]
        plate = platetransfer["result"]["plate"]

        begin
          update_aliquots_in_sequencescape(plate["uuid"], plate["wells"])
        rescue Sequel::Rollback => e
          metadata.reject(:requeue => true)
          puts "Error updating plate aliquots in Sequencescape: #{e}"
        else
          metadata.ack
        end
      end

      # Create a plate in Sequencescape database.
      # The following tables are updated:
      # - Assets (the plate is saved with a Unassigned plate purpose)
      # - Assets (each well of the plate are saved with the right map_id)
      # - Uuids (the external id is S2 uuid)
      # - Container_associations (to link each well to the plate in Assets)
      # If the transaction fails, it raises a Sequel::Rollback exception and 
      # the transaction rollbacks.
      # @param [String] plate json
      def create_plate_in_sequencescape(plate_json)
        plate = JSON.parse(plate_json)["plate"]

        t_assets = @db[:assets]
        t_container_associations = @db[:container_associations]
        t_maps = @db[:maps]
        t_uuids = @db[:uuids]

        @db.transaction do
          # Save plate
          plate_uuid = plate["uuid"]
          plate_id = t_assets.insert(:sti_type => PLATE, :plate_purpose_id => UNASSIGNED_PLATE_PURPOSE_ID) 
          t_uuids.insert(:resource_type => ASSET, :resource_id => plate_id, :external_id => plate_uuid) 

          # Save wells
          asset_size = plate["number_of_rows"] * plate["number_of_columns"]
          plate["wells"].each do |well|
            map_id = t_maps.select(:id).where(:description => well.first, :asset_size => asset_size).first[:id]
            well_id = t_assets.insert(:sti_type => WELL, :map_id => map_id) 
            t_container_associations.insert(:container_id => plate_id, :content_id => well_id) 
          end
        end
      end 

      # Update plate purpose in Sequencescape.
      # If the plate_uuid is not found in the database,
      # it means the order message has been received before 
      # the plate message. A PlateNotFoundInSequencescape exception 
      # is raised in that case. Otherwise, the plate is updated 
      # with the right plate_purpose_id for a stock plate.
      # @param [String] plate uuid
      def update_plate_purpose_in_sequencescape(plate_uuid)
        t_assets = @db[:assets]
        t_uuids = @db[:uuids]

        @db.transaction do
          plate_id = t_uuids.select(:resource_id).where(:external_id => plate_uuid).first[:resource_id]
          raise PlateNotFoundInSequencescape if plate_id.nil?

          t_assets.where(:id => plate_id).update(:plate_purpose_id => STOCK_PLATE_PURPOSE_ID) 
        end
      end

      # Delete plates and their informations in Sequencescape
      # database if the plate appears in item order and is not
      # a stock plate.
      # @param [Hash] plates
      def delete_unassigned_plates_in_sequencescape(plates)
        plates.each do |_, item|
          t_assets = @db[:assets]
          t_uuids = @db[:uuids]
          t_container_associations = @db[:container_associations]
          plate_uuid = item["uuid"]

          begin
          @db.transaction do
            plate = t_assets.select(:assets__id).join(:uuids, :resource_id => :id).where(:external_id => plate_uuid).first
            unless plate.nil?
              # Delete wells in assets
              well_ids = container_associations.select(:assets__id).join(:assets, :id => :content_id).where(:container_id => id).all.inject([]) {|m,e| m << e[:id]}
              t_assets.where(:id => well_ids).delete
              # Delete container_associations
              t_container_associations.where(:content_id => wells_id).delete
              # Delete plate in assets
              t_assets.where(:id => plate[:id]).delete
              # Delete plate uuid
              t_uuis.where(:external_id => plate_uuid).delete
            end
          end
          rescue Sequel::Rollback => e
          end
        end
      end



      def update_aliquots_in_sequencescape(plate_uuid, s2_wells)
        t_assets = @db[:assets]
        t_aliquots = @db[:aliquots]
        t_samples = @db[:samples]

        s2_wells.each do |location, aliquots|
          unless aliquots.empty?
            plate_id = t_assets.select(:id).join(:uuids, :external_id => :id).where(:id => plate_uuid)

            aliquots.each do |aliquot|
#              t_assets.select(:assets.*).join(:uuids, :resource_id => :id).where(:external_id => uuid).first[:id]

            end
          end
        end
      end
    end
  end
end
