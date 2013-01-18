require 'lims-core/laboratory'
require 'lims-core/organization'
require 'json'

module Lims::MessageBusClient
  module SequencescapeStockPlateCreator
    # Json Decoder which decodes a S2 json message into a
    # Lims Core Resource.
    module JsonDecoder

      # Exception raised if a decoder for a specific 
      # model is undefined.
      class UndefinedDecoder < StandardError
      end

      # Get the decoder for the model in parameters
      # @param [String] model
      def json_decoder_for(model)
        begin
          decoder = "#{model.to_s.capitalize.sub(/_./) {|p| p[1].upcase}}JsonDecoder"
          eval(decoder) 
        rescue NameError => e
          raise UndefinedDecoder, "#{decoder} is undefined"
        end
      end 


      module PlateJsonDecoder
        # Create a Core Laboratory Plate from the json
        # @param [String] json
        # @return [Hash] hash
        # @example
        # {:plate => Lims::Core::Laboratory::Plate, 
        #  :uuid => "plate_uuid", 
        #  :sample_uuids => {"A1" => ["sample_uuid"]}}
        def self.call(json)
          plate_hash = json["plate"]
          plate = Lims::Core::Laboratory::Plate.new({:number_of_rows => plate_hash["number_of_rows"],
                                                     :number_of_columns => plate_hash["number_of_columns"]})   
          plate_hash["wells"].each do |location, aliquots|
            unless aliquots.empty?
              aliquots.each do |aliquot|
                plate[location] << Lims::Core::Laboratory::Aliquot.new
              end
            end
          end

          {:plate => plate, 
           :uuid => plate_hash["uuid"], 
           :sample_uuids => sample_uuids(plate_hash["wells"])}
        end

        # Get the sample uuids in the plate
        # @param [Hash] wells
        # @return [Hash] sample uuids
        # @example
        # {"A1" => ["sample_uuid1", "sample_uuid2"]} 
        def self.sample_uuids(wells)
          {}.tap do |uuids|
            wells.each do |location, aliquots|
              unless aliquots.empty?
                aliquots.each do |aliquot|
                  uuids[location] ||= []
                  uuids[location] << aliquot["sample"]["uuid"]
                end
              end
            end
          end
        end
      end


      module OrderJsonDecoder
        def self.call(json)
          order_h = json["order"]
          order = Lims::Core::Organization::Order.new
          order_h["items"].each do |role, settings|
            order[role] = Lims::Core::Organization::Order::Item.new({
              :uuid => settings["uuid"],
              :status => settings["status"]})
          end

          {:order => order, :uuid => order_h["uuid"]}
        end
      end


      module PlateTransferJsonDecoder
        def self.call(json)
          transfer_h = json["plate_transfer"]
          PlateJsonDecoder.call(transfer_h["result"])         
        end
      end
    end
  end
end
