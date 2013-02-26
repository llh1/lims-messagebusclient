require 'sequel'
require 'sequel/adapters/mysql'

module Lims::MessageBusClient
  module SequencescapeStockPlateCreator
    module SequencescapeUpdater

      WELL = "Well"
      PLATE = "Plate"
      ASSET = "Asset"
      SAMPLE = "Sample"
      STOCK_PLATE_PURPOSE_ID = 2
      UNASSIGNED_PLATE_PURPOSE_ID = 100
      STOCK_PLATES = ["Stock RNA", "Stock DNA"]
      ITEM_DONE_STATUS = "done"

      # Exception raised after an unsuccessful lookup for a plate 
      # in Sequencescape database.
      class PlateNotFoundInSequencescape < StandardError
      end

      def self.included(klass)
        klass.class_eval do
          include Virtus
          include Aequitas
          attribute :mysql_settings, Hash, :required => true, :writer => :private, :reader => :private
          attribute :db, Sequel::MySQL::Database, :required => true, :writer => :private, :reader => :private
        end
      end
      
      # Setup the Sequencescape database connection
      # @param [Hash] MySQL settings
      def sequencescape_db_setup(settings = {})
        @mysql_settings = settings
        @db = Sequel.connect(:adapter => mysql_settings['adapter'],
                             :host => mysql_settings['host'],
                             :user => mysql_settings['user'],
                             :password => mysql_settings['password'],
                             :database => mysql_settings['database'])
      end 

      # Create a plate in Sequencescape database.
      # The following tables are updated:
      # - Assets (the plate is saved with a Unassigned plate purpose)
      # - Assets (each well of the plate are saved with the right map_id)
      # - Uuids (the external id is S2 uuid)
      # - Container_associations (to link each well to the plate in Assets)
      # If the transaction fails, it raises a Sequel::Rollback exception and 
      # the transaction rollbacks.
      # @param [Lims::Core::Laboratory::Plate] plate
      # @param [String] plate uuid
      # @param [Hash] sample uuids
      def create_plate_in_sequencescape(plate, plate_uuid, sample_uuids)
        @db.transaction do
          # Save plate and plate uuid
          plate_id = db[:assets].insert(
            :sti_type => PLATE, 
            :plate_purpose_id => UNASSIGNED_PLATE_PURPOSE_ID
          ) 

          db[:uuids].insert(
            :resource_type => ASSET, 
            :resource_id => plate_id, 
            :external_id => plate_uuid
          ) 

          # Save wells and set the associations with the plate
          asset_size = plate.number_of_rows * plate.number_of_columns
          plate.keys.each do |location|
            map_id = db[:maps].select(:id).where(
              :description => location, 
              :asset_size => asset_size
            ).first[:id]

            well_id = db[:assets].insert(
              :sti_type => WELL, 
              :map_id => map_id
            ) 

            db[:container_associations].insert(
              :container_id => plate_id, 
              :content_id => well_id
            ) 

            # Save well aliquots
            if sample_uuids.has_key?(location)
              sample_uuids[location].each do |sample_uuid|
                sample_id = db[:uuids].select(:resource_id).where(
                  :resource_type => SAMPLE, 
                  :external_id => sample_uuid
                ).first[:resource_id] 

                db[:aliquots].insert(
                  :receptacle_id => well_id, 
                  :sample_id => sample_id
                )
              end
            end 
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
        @db.transaction do
          plate_id = db[:uuids].select(:resource_id).where(
            :external_id => plate_uuid
          ).first[:resource_id]

          raise PlateNotFoundInSequencescape if plate_id.nil?

          db[:assets].where(:id => plate_id).update(
            :plate_purpose_id => STOCK_PLATE_PURPOSE_ID
          ) 
        end
      end

      # Delete plates and their informations in Sequencescape
      # database if the plate appears in item order and is not
      # a stock plate.
      # @param [Hash] non stock plates
      def delete_unassigned_plates_in_sequencescape(s2_items)
        s2_items.flatten.each do |item|
          begin
            @db.transaction do
              plate = db[:assets].select(:assets__id).join(
                :uuids, 
                :resource_id => :id
              ).where(:external_id => item.uuid).first

              unless plate.nil?
                # Delete wells in assets
                well_ids = db[:container_associations].select(:assets__id).join(
                  :assets, 
                  :id => :content_id
                ).where(:container_id => plate[:id]).all.inject([]) do |m,e| 
                  m << e[:id]
                end
                db[:assets].where(:id => well_ids).delete

                # Delete aliquots
                db[:aliquots].where(:receptacle_id => well_ids).delete

                # Delete container_associations
                db[:container_associations].where(:content_id => well_ids).delete

                # Delete plate in assets
                db[:assets].where(:id => plate[:id]).delete

                # Delete plate uuid
                db[:uuids].where(:external_id => item.uuid).delete
              end
            end
          rescue Sequel::Rollback => e
          end
        end
      end

      # Update the aliquots of a plate after a plate transfer
      # @param [Lims::Core::Laboratory::Plate] plate
      # @param [String] plate uuid
      # @param [Hash] sample uuids
      def update_aliquots_in_sequencescape(plate, plate_uuid, sample_uuids)
        plate_id = db[:uuids].select(:resource_id).where(
          :external_id => plate_uuid
        ).first[:resource_id]

        if plate_id
          wells = db[:container_associations].select(
            :assets__id, 
            :maps__description
          ).join(
            :assets, 
            :id => :content_id
          ).join(
            :maps, 
            :id => :map_id
          ).where(:container_id => plate_id).all.inject({}) do |m,e|
            m.merge({e[:description] => e[:id]})
          end

          db[:aliquots].where(:receptacle_id => wells.values).delete

          plate.keys.each do |location|
            if sample_uuids.has_key?(location)
              sample_uuids[location].each do |sample_uuid|
                sample_id = db[:uuids].select(:resource_id).where(
                  :resource_type => SAMPLE, 
                  :external_id => sample_uuid
                ).first[:resource_id]

                db[:aliquots].insert(
                  :receptacle_id => wells[location], 
                  :sample_id => sample_id
                )
              end
            end
          end
        end
      end
    end
  end 
end
