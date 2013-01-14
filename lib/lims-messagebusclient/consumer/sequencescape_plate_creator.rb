require 'json'
require 'sequel'
require 'lims-messagebusclient/consumer'

module Lims
  module MessageBusClient
    class SequencescapePlateCreator
      include Consumer
      attribute :creator_name, String, :required => true, :writer => :private
      attribute :mysql_settings, Hash, :required => true, :writer => :private
      attribute :message_buffer, Hash, :writer => :private
    
      WELL = "Well"
      PLATE = "Plate"

      def initialize(creator_name, amqp_settings, mysql_settings)
        setup(amqp_settings)
        @creator_name = creator_name
        @mysql_settings = mysql_settings
        @db = Sequel.connect(:adapter => mysql_settings['adapter'],
                             :host => mysql_settings['host'],
                             :user => mysql_settings['user'],
                             :password => mysql_settings['password'],
                             :database => mysql_settings['database'])
        @db = Sequel.connect('mysql://root:root@localhost:3306/sequencescape_development')
        set_creator_queue
      end

      def routing_keys
        ["pipeline.user.plate.create"]
      end

      def set_creator_queue
        self.add_queue(creator_name, routing_keys) do |metadata, payload|
          metadata.ack
          create_plate(payload)
        end
      end

      def create_plate(plate)
        plate = JSON.parse(plate)["plate"]

        t_assets = @db[:assets]
        t_container_associations = @db[:container_associations]
        t_maps = @db[:maps]

        # Save plate
        plate_id = t_assets.insert(:sti_type => PLATE, 
                                   :plate_purpose_id => 2) 


        # Save wells
        asset_size = plate["number_of_rows"] * plate["number_of_columns"]
        plate["wells"].each do |well|
          map_id = t_maps.select(:id).where(:description => well.first, :asset_size => asset_size)
          well_id = t_assets.insert(:sti_type => WELL, :map_id => map_id) 
          t_container_associations.insert(:container_id => plate_id, :content_id => well_id) 
        end
      end 

      private :set_creator_queue, :routing_keys

    end
  end
end
