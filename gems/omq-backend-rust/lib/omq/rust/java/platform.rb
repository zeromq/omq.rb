# frozen_string_literal: true

require "rbconfig"

require_relative "../version"

module OMQ
  module Rust
    module Java
      class UnsupportedPlatformError < LoadError; end

      class << self
        def classifier
          override = ENV.fetch("OMQ_JAVA_CLASSIFIER", "").strip
          return validate_classifier(override) unless override.empty?

          classifier_for(os_name: host_os_name, os_arch: host_os_arch)
        end


        def classifier_for(os_name:, os_arch:)
          os   = normalize_os(os_name)
          arch = normalize_arch(os_arch)

          classifier = "#{os}-#{arch}" if os && arch
          return classifier if OMQ::Rust::OMQ_JAVA_CLASSIFIERS.include?(classifier)

          raise UnsupportedPlatformError,
                "unsupported OMQ.java platform: #{os_name.inspect}/#{os_arch.inspect} " \
                "(supported: #{OMQ::Rust::OMQ_JAVA_CLASSIFIERS.join(", ")})"
        end


        private


        def validate_classifier(classifier)
          return classifier if OMQ::Rust::OMQ_JAVA_CLASSIFIERS.include?(classifier)

          raise UnsupportedPlatformError,
                "unsupported OMQ.java classifier: #{classifier.inspect} " \
                "(supported: #{OMQ::Rust::OMQ_JAVA_CLASSIFIERS.join(", ")})"
        end


        def host_os_name
          java_system_property("os.name") ||
            RbConfig::CONFIG["host_os"] ||
            RbConfig::CONFIG["target_os"] ||
            RUBY_PLATFORM
        end


        def host_os_arch
          java_system_property("os.arch") ||
            RbConfig::CONFIG["host_cpu"] ||
            RbConfig::CONFIG["target_cpu"] ||
            RUBY_PLATFORM
        end


        def java_system_property(name)
          return unless RUBY_ENGINE == "jruby"

          require "java"
          ::Java::JavaLang::System.get_property(name)
        rescue LoadError, NameError
          nil
        end


        def normalize_os(value)
          case value.to_s.downcase
          when /linux/
            "linux"
          when /darwin|mac\s*os|macos/
            "macos"
          when /windows|mswin|mingw|cygwin/
            "windows"
          end
        end


        def normalize_arch(value)
          case value.to_s.downcase
          when "amd64", "x64", "x86-64", "x86_64"
            "x86_64"
          when "aarch64", "arm64"
            "aarch64"
          end
        end
      end
    end
  end
end
