# frozen_string_literal: true

module OMQ
  module CLI
    # Compiles and evaluates a single Ruby expression string for use in
    # --recv-eval / --send-eval. Handles BEGIN{}/END{} block extraction,
    # proc compilation, and result normalisation.
    #
    # One instance per direction (send or recv).
    #
    class ExpressionEvaluator
      attr_reader :begin_proc, :end_proc, :eval_proc

      # Sentinel: eval proc returned the context object, meaning it already
      # sent the reply itself.
      SENT = Object.new.freeze


      # @param src [String, nil]  the raw expression string (may include BEGIN{}/END{})
      # @param format [Symbol]    the active format, used to normalise results
      # @param fallback_proc [Proc, nil]  registered OMQ.outgoing/incoming handler;
      #   used only when +src+ is nil (no inline expression)
      #
      def initialize(src, format:, fallback_proc: nil)
        @format = format

        if src
          expr, begin_body, end_body = extract_blocks(src)
          @begin_proc = eval("proc { #{begin_body} }") if begin_body
          @end_proc   = eval("proc { #{end_body} }")   if end_body

          if expr && !expr.strip.empty?
            @eval_proc = eval("proc { #{expr} }")
          end
        elsif fallback_proc
          @eval_proc = proc { fallback_proc.call(it) }
        end
      end


      # Runs the eval proc against +parts+ using +context+ as self.
      # Returns the normalised result Array, nil (filter/skip), or SENT.
      #
      def call(parts, context)
        return parts unless @eval_proc

        result = context.instance_exec(parts, &@eval_proc)
        return nil  if result.nil?
        return SENT if result.equal?(context)
        return result if @format == :marshal

        result = result.is_a?(Array) ? result : [result]
        result.map { |part| part.to_s }
      rescue => e
        $stderr.puts "omq: eval error: #{e.message} (#{e.class})"
        exit 3
      end


      # Normalises an eval result to nil (skip), an Array (text formats),
      # or an arbitrary Ruby object (+:marshal+).
      #
      # Used inside Ractor worker blocks where instance methods are unavailable.
      # When +format+ is :marshal, the raw result is passed through — the
      # wire path will Marshal.dump it into a single frame.
      #
      def self.normalize_result(result, format: nil)
        return nil if result.nil?
        return result if format == :marshal
        result = result.is_a?(Array) ? result : [result]
        result.map { |part| part.to_s }
      end


      # Compiles begin/end/eval procs inside a Ractor from a raw expression
      # string. Returns [begin_proc, end_proc, eval_proc], any may be nil.
      #
      # Must be called inside the Ractor block (Procs are not Ractor-shareable).
      #
      def self.compile_inside_ractor(src)
        return [nil, nil, nil] unless src

        expr, begin_body = extract_block(src,  "BEGIN")
        expr, end_body   = extract_block(expr, "END")

        begin_proc = eval("proc { #{begin_body} }") if begin_body
        end_proc   = eval("proc { #{end_body} }")   if end_body
        eval_proc  = nil

        if expr && !expr.strip.empty?
          eval_proc = eval("proc { #{expr} }")
        end

        [begin_proc, end_proc, eval_proc]
      end


      # Strips a +BEGIN {...}+ or +END {...}+ block from +expr+ and
      # returns +[trimmed_expr, block_body_or_nil]+. Brace-matched scan,
      # so nested `{}` inside the block body are handled. Shared by
      # instance and Ractor compile paths, so must be a class method
      # (Ractors cannot call back into instance state).
      #
      def self.extract_block(expr, keyword)
        start = expr.index(/#{keyword}\s*\{/) or return [expr, nil]

        i     = expr.index("{", start)
        depth = 1
        j     = i + 1

        while j < expr.length && depth > 0
          case expr[j]
          when "{"
            depth += 1
          when "}"
            depth -= 1
          end

          j += 1
        end

        body    = expr[(i + 1)..(j - 2)]
        trimmed = expr[0...start] + expr[j..]
        [trimmed, body]
      end


      private


      def extract_blocks(expr)
        expr, begin_body = self.class.extract_block(expr, "BEGIN")
        expr, end_body   = self.class.extract_block(expr, "END")
        [expr, begin_body, end_body]
      end
    end
  end
end
