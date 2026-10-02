# frozen_string_literal: true

module SolidJobs
  module Utilities
    module_function

    def stringify_keys(value)
      return stringify_hash_keys(value).transform_values { |nested| stringify_keys(nested) } if value.is_a?(Hash)
      return value.map { |nested| stringify_keys(nested) } if value.is_a?(Array)

      value
    end

    def stringify_hash_keys(hash)
      hash.transform_keys(&:to_s)
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
