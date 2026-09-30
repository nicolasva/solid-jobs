# frozen_string_literal: true

module SolidJobs
  module Utilities
    module_function

    def stringify_keys(value)
      case value
      when Hash
        value.each_with_object({}) do |(key, nested), result|
          result[key.to_s] = stringify_keys(nested)
        end
      when Array
        value.map { |nested| stringify_keys(nested) }
      else
        value
      end
    end

    def stringify_hash_keys(hash)
      hash.each_with_object({}) { |(key, value), result| result[key.to_s] = value }
    end

    def shareable_copy(value)
      return value if Ractor.shareable?(value)

      copied = case value
      when Hash
        value.each_with_object({}) do |(key, nested), result|
          result[shareable_copy(key)] = shareable_copy(nested)
        end
      when Array
        value.map { |nested| shareable_copy(nested) }
      when String
        value.dup.freeze
      when Symbol, Numeric, true, false, nil
        value
      else
        value.frozen? ? value : value.dup.freeze
      end
      Ractor.make_shareable(copied)
    end

    def constantize(name)
      name.split("::").reject(&:empty?).reduce(Object) { |scope, part| scope.const_get(part, false) }
    end

    def realtime_milliseconds
      ::Process.clock_gettime(::Process::CLOCK_REALTIME, :millisecond)
    end
  end
end
