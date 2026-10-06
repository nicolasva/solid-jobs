# frozen_string_literal: true

require "socket"
require "uri"

class RedisFaultProxy
  attr_reader :port

  def initialize(target_url)
    uri = URI(target_url)
    @target_host = uri.host
    @target_port = uri.port
    @mutex = Mutex.new
    @connections = []
    @enabled = true
    @drop_command = nil
    @delay_command = nil
    @delay_seconds = nil
  end

  def start
    @server = TCPServer.new("127.0.0.1", 0)
    @port = @server.local_address.ip_port
    @acceptor = Thread.new { accept_connections }
    self
  end

  def url
    "redis://127.0.0.1:#{port}/0"
  end

  def cut!
    @mutex.synchronize do
      @enabled = false
      @connections.each { |socket| socket.close rescue nil }
      @connections.clear
    end
  end

  def restore!
    @mutex.synchronize { @enabled = true }
  end

  def drop_next_response_for!(command)
    @mutex.synchronize { @drop_command = command.to_s.upcase }
  end

  def delay_next_response_for!(command, seconds)
    @mutex.synchronize do
      @delay_command = command.to_s.upcase
      @delay_seconds = Float(seconds)
    end
  end

  def stop
    @server&.close
    cut!
    @acceptor&.join
  rescue IOError
    nil
  end

  private

  def accept_connections
    loop do
      client = @server.accept
      enabled = @mutex.synchronize { @enabled }
      unless enabled
        client.close
        next
      end
      upstream = TCPSocket.new(@target_host, @target_port)
      track(client, upstream)
      Thread.new { proxy_connection(client, upstream) }
    end
  rescue IOError, Errno::EBADF
    nil
  end

  def proxy_connection(client, upstream)
    state = {drop_response: false, delay_response: nil}
    request = Thread.new { forward_requests(client, upstream, state) }
    response = Thread.new { forward_responses(upstream, client, state) }
    request.join
    response.join
  ensure
    untrack(client, upstream)
    client.close rescue nil
    upstream.close rescue nil
  end

  def forward_requests(client, upstream, state)
    buffer = +""
    loop do
      chunk = client.readpartial(16_384)
      buffer << chunk
      command = @mutex.synchronize { @drop_command }
      if command && buffer.include?("\r\n$#{command.bytesize}\r\n#{command}\r\n")
        @mutex.synchronize do
          if @drop_command == command
            @drop_command = nil
            state[:drop_response] = true
          end
        end
        delay_command, delay_seconds = @mutex.synchronize { [@delay_command, @delay_seconds] }
        if delay_command && buffer.include?("\r\n$#{delay_command.bytesize}\r\n#{delay_command}\r\n")
          @mutex.synchronize do
            if @delay_command == delay_command
              @delay_command = @delay_seconds = nil
              state[:delay_response] = delay_seconds
            end
          end
        end
      end
      buffer = buffer.byteslice(-128, 128) || buffer
      upstream.write(chunk)
    end
  rescue EOFError, IOError, SystemCallError
    nil
  ensure
    upstream.close_write rescue nil
  end

  def forward_responses(upstream, client, state)
    loop do
      chunk = upstream.readpartial(16_384)
      if (delay = state.delete(:delay_response))
        sleep delay
      end
      if state[:drop_response]
        client.close
        upstream.close
        break
      end
      client.write(chunk)
    end
  rescue EOFError, IOError, SystemCallError
    nil
  ensure
    client.close_write rescue nil
  end

  def track(*sockets)
    @mutex.synchronize { @connections.concat(sockets) }
  end

  def untrack(*sockets)
    @mutex.synchronize { sockets.each { |socket| @connections.delete(socket) } }
  end
end
