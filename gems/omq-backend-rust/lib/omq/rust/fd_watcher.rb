# frozen_string_literal: true

module OMQ
  module Rust
    class FdWatcher
      Watch = Struct.new(:id, :io, :owner, :once, :callback, keyword_init: true)

      class << self
        def watch_once(fd, owner:, &block)
          watch(fd, owner: owner, once: true, &block)
        end


        def watch_loop(fd, owner:, &block)
          watch(fd, owner: owner, once: false, &block)
        end


        def unwatch_owner(owner)
          removed = nil

          mutex.synchronize do
            reset_after_fork
            removed = watchers.values.select { |watch| watch.owner.equal?(owner) }
            removed.each { |watch| watchers.delete(watch.id) }
          end

          removed.each { |watch| close_io(watch.io) }
          wake
        end


        private


        def watch(fd, owner:, once:, &block)
          raise ArgumentError, "block required" unless block

          io = IO.for_fd(fd, autoclose: false)
          mutex.synchronize do
            reset_after_fork
            ensure_started
            id = next_id
            watchers[id] = Watch.new(id: id, io: io, owner: owner, once: once, callback: block)
            id
          end
        rescue StandardError
          close_io(io)
          raise
        ensure
          wake if io
        end


        def reset_after_fork
          pid = Process.pid
          return if @pid == pid

          watchers.each_value { |watch| close_io(watch.io) }
          close_io(@wake_r)
          close_io(@wake_w)
          @watchers = {}
          @wake_r   = nil
          @wake_w   = nil
          @thread   = nil
          @next_id  = 0
          @pid      = pid
        end


        def ensure_started
          return if @thread&.alive?

          @wake_r, @wake_w = IO.pipe
          @thread = Thread.new do
            Thread.current.name = "omq-rust-watch" if Thread.current.respond_to?(:name=)
            run
          end
        end


        def run
          loop do
            wake_io, current = snapshot
            readable = [wake_io, *current.map(&:io)].reject(&:closed?)
            ready = IO.select(readable)&.first || []

            drain_wake(wake_io) if ready.include?(wake_io)

            current.each do |watch|
              next unless ready.include?(watch.io)

              dispatch(watch)
            end
          rescue IOError, SystemCallError
            sweep_closed
          end
        end


        def dispatch(watch)
          active = watch.once ? delete_if_current(watch) : current?(watch)
          return unless active

          watch.callback.call(watch.io)
        rescue StandardError
          delete_if_current(watch)
        ensure
          close_io(watch.io) if watch.once
        end


        def snapshot
          mutex.synchronize { [@wake_r, watchers.values] }
        end


        def current?(watch)
          mutex.synchronize { watchers[watch.id].equal?(watch) }
        end


        def delete_if_current(watch)
          mutex.synchronize do
            if watchers[watch.id].equal?(watch)
              watchers.delete(watch.id)
              true
            else
              false
            end
          end
        end


        def sweep_closed
          removed = nil
          mutex.synchronize do
            removed = watchers.values.select { |watch| watch.io.closed? }
            removed.each { |watch| watchers.delete(watch.id) }
          end
          removed.each { |watch| close_io(watch.io) }
        end


        def drain_wake(io)
          loop do
            result = io.read_nonblock(256, exception: false)
            break if result == :wait_readable || result.nil? || result.empty?
          end
        end


        def wake
          return unless @wake_w && !@wake_w.closed?

          @wake_w.write_nonblock(".", exception: false)
        rescue IOError, SystemCallError
        end


        def close_io(io)
          io.close if io && !io.closed?
        rescue IOError, SystemCallError
        end


        def watchers
          @watchers ||= {}
        end


        def mutex
          @mutex ||= Mutex.new
        end


        def next_id
          @next_id ||= 0
          @next_id += 1
        end
      end
    end
  end
end
