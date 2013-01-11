require 'lims-messagebusclient'

module Lims::MessageBusClient
  describe Auditor do
    let(:auditor_name) { "audit" }

    let(:host) { "host" }
    let(:port) { "port" }
    let(:user) { "user" }
    let(:password) { "password" }
    let(:exchange_name) { "exchange name" }
    let(:durable) { "durable" }
    let(:amqp_settings) {{
      "host" => host,
      "port" => port,
      "user" => user,
      "password" => password,
      "exchange_name" => exchange_name,
      "durable" => durable
    }}

    let(:audit_file) { "audit/file" }

    context "to be valid" do
      it "requires a name" do 
        described_class.new("", amqp_settings, audit_file).valid?.should == false
      end

      it "requires a file path" do 
        described_class.new(auditor_name, amqp_settings, "").valid?.should == false
      end

      it "requires a RabbitMQ host" do
        described_class.new(auditor_name, amqp_settings - ["host"], audit_file).valid?.should == false
      end

      it "requires a port" do
        described_class.new(auditor_name, amqp_settings - ["port"], audit_file).valid?.should == false
      end
      it "requires an exchange name " do
        described_class.new(auditor_name, amqp_settings - ["exchange_name"], audit_file).valid?.should == false
      end

      it "requires the durable option" do
        described_class.new(auditor_name, amqp_settings - ["durable"], audit_file).valid?.should == false
      end
    end

    context "invalid" do
      it "sends an exception if parameters are invalid" do
        expect do
          described_class.new("", amqp_settings, audit_file).start
        end.to raise_error(Consumer::InvalidSettingsError)
      end
    end
  end
end
