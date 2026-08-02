# frozen_string_literal: true

module OMQ
  module CLI
    # Shared Ractor infrastructure for parallel worker modes.
    module RactorHelpers
      # Sentinel value sent through ports to signal consumer threads to exit.
      # Port#close does not unblock a waiting #receive, so we must send an
      # explicit shutdown marker.
      SHUTDOWN = :__omq_shutdown__


      # Resolves TCP hostnames to IP addresses so Ractors don't touch
      # Resolv::DefaultResolver (which is not shareable).
      #
      def self.preresolve_tcp(endpoints)
        endpoints.flat_map do |ep|
          url = ep.url
          if url.start_with?("tcp://")
            host, port = OMQ::Transport::TCP.parse_endpoint(url)
            Addrinfo.getaddrinfo(host, port, nil, :STREAM).map do |addr|
              ip = addr.ip_address
              ip = "[#{ip}]" if ip.include?(":")
              Endpoint.new("tcp://#{ip}:#{addr.ip_port}", ep.bind?)
            end
          else
            ep
          end
        end
      end


      # Starts a Ractor::Port and a consumer thread that drains log
      # messages to stderr sequentially. Returns [port, thread].
      # Send SHUTDOWN through the port to stop the consumer.
      #
      def self.start_log_consumer
        port = Ractor::Port.new
        thread = Thread.new(port) do |p|
          loop do
            msg = p.receive
            break if msg.equal?(SHUTDOWN)
            $stderr.write("#{msg}\n")
          rescue Ractor::ClosedError
            break
          end
        end
        [port, thread]
      end


      # Starts a Ractor::Port and a consumer thread that drains
      # formatted output to stdout sequentially. Returns [port, thread].
      # Send SHUTDOWN through the port to stop the consumer.
      #
      def self.start_output_consumer
        port = Ractor::Port.new
        thread = Thread.new(port) do |p|
          loop do
            msg = p.receive
            break if msg.equal?(SHUTDOWN)
            $stdout.write(msg)
          rescue Ractor::ClosedError
            break
          end
        end
        [port, thread]
      end


      # Sends the shutdown sentinel and joins the consumer thread.
      #
      def self.stop_consumer(port, thread)
        port.send(SHUTDOWN)
        thread.join
      rescue Ractor::ClosedError
        thread.join(1)
      end
    end
  end
end
