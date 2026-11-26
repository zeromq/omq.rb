# frozen_string_literal: true

module OMQ
  module Routing
    # The subscription prefixes one peer holds, as a radix trie.
    #
    # {#match?} walks the topic's bytes once, so its cost depends on the
    # topic, not on how many prefixes the peer holds (libzmq and omq.rs
    # use tries too). Bytes are compared without regard to encoding, so a
    # UTF-8 topic matches a binary prefix.
    #
    # Like libzmq's PUB side, subscribing a prefix twice stores it once.
    #
    class PrefixSet
      include Enumerable

      # A trie node. +label+ is the edge from the parent, +children+ maps
      # the first byte of each child's label to the child (nil for a leaf).
      Node = Struct.new(:label, :children, :terminal)


      # @return [Integer] number of prefixes
      #
      attr_reader :size


      def initialize
        @root = Node.new(::Protocol::ZMTP::Codec::EMPTY_BINARY, nil, false)
        @size = 0
      end


      # @param prefix [String]
      # @return [Boolean] true if the prefix was new
      #
      def add(prefix)
        node = @root
        i    = 0
        n    = prefix.bytesize

        while i < n
          children = (node.children ||= {})
          byte     = prefix.getbyte(i)
          child    = children[byte]

          unless child
            children[byte] = Node.new(prefix.byteslice(i, n - i).b.freeze, nil, true)
            @size         += 1
            return true
          end

          shared = common_length(child.label, prefix, i)
          split(child, shared) if shared < child.label.bytesize

          node = child
          i   += shared
        end

        return false if node.terminal

        node.terminal = true
        @size        += 1
        true
      end


      alias << add


      # @param prefix [String]
      # @return [Boolean] true if the prefix was present
      #
      def delete(prefix)
        parent = nil
        node   = find(prefix) { |p| parent = p }
        return false unless node&.terminal

        node.terminal = false
        @size        -= 1
        prune(parent, node)
        true
      end


      # @param prefix [String]
      # @return [Boolean]
      #
      def include?(prefix)
        node = find(prefix)
        node ? node.terminal : false
      end


      # @return [Boolean]
      #
      def empty?
        @size.zero?
      end


      # @param topic [String]
      # @return [Boolean] true if some prefix is a prefix of +topic+
      #
      def match?(topic)
        node = @root
        i    = 0
        n    = topic.bytesize

        while true
          return true if node.terminal
          return false if i == n

          children = node.children
          return false unless children

          child = children[topic.getbyte(i)]
          return false unless child

          label = child.label
          m     = label.bytesize
          return false if i + m > n

          j = 1 # the first byte matched through the Hash

          while j < m
            return false unless label.getbyte(j) == topic.getbyte(i + j)
            j += 1
          end

          node = child
          i   += m
        end
      end


      # Yields every prefix (new frozen binary strings).
      #
      # @yieldparam prefix [String]
      #
      def each(&block)
        return enum_for(:each) { @size } unless block

        walk(@root, ::Protocol::ZMTP::Codec::EMPTY_BINARY, &block)
        self
      end


      private


      # Number of bytes +label+ shares with +prefix+ from +offset+ on.
      #
      def common_length(label, prefix, offset)
        j = 0
        m = label.bytesize
        n = prefix.bytesize - offset

        j += 1 while j < m && j < n && label.getbyte(j) == prefix.getbyte(offset + j)
        j
      end


      # Cuts +node+'s label after +at+ bytes; the rest moves to a new child.
      #
      def split(node, at)
        label         = node.label
        lower         = Node.new(label.byteslice(at, label.bytesize - at).freeze, node.children, node.terminal)
        node.label    = label.byteslice(0, at).freeze
        node.children = { lower.label.getbyte(0) => lower }
        node.terminal = false
      end


      # Exact lookup. Yields each node passed on the way (the last one
      # yielded is the parent of the returned node).
      #
      def find(prefix)
        node = @root
        i    = 0
        n    = prefix.bytesize

        while i < n
          children = node.children
          return unless children

          child = children[prefix.getbyte(i)]
          return unless child

          m = child.label.bytesize
          return if i + m > n || common_length(child.label, prefix, i) < m

          yield node if block_given?
          node = child
          i   += m
        end

        node
      end


      # Removes +node+ if it became useless, then merges a parent left
      # with a single child, so the trie stays compact.
      #
      def prune(parent, node)
        return if node.equal?(@root)

        if node.children.nil?
          parent.children.delete(node.label.getbyte(0))
          parent.children = nil if parent.children.empty?
          merge(parent) unless parent.equal?(@root) || parent.terminal
        else
          merge(node)
        end
      end


      def merge(node)
        children = node.children
        return unless children&.size == 1

        child         = children.each_value.first
        node.label    = (node.label + child.label).freeze
        node.children = child.children
        node.terminal = child.terminal
      end


      def walk(node, path, &block)
        path = (path + node.label).freeze unless node.equal?(@root)
        yield path if node.terminal

        node.children&.each_value do |child|
          walk(child, path, &block)
        end
      end

    end
  end
end
