# frozen_string_literal: true

module SolidJobs
  class InterceptorRegistry
    Definition = Data.define(:type, :arguments)

    include Enumerable

    def initialize(definitions = [])
      @definitions = definitions.map do |definition|
        Definition.new(definition.type, definition.arguments.dup.freeze)
      end
    end

    def each(&block)
      @definitions.each(&block)
    end

    def use(type, *arguments)
      discard(type)
      @definitions = (@definitions + [Definition.new(type, arguments.freeze)]).freeze
      self
    end

    def discard(type)
      @definitions = @definitions.reject { |definition| definition.type == type }.freeze
      self
    end

    def empty?
      @definitions.empty?
    end

    def call(context, &operation)
      continuation = operation
      @definitions.reverse_each do |definition|
        interceptor = definition.type.new(*definition.arguments)
        downstream = continuation
        continuation = -> { interceptor.around(context, &downstream) }
      end
      continuation.call
    end

    def export
      Utilities.shareable_copy(
        @definitions.map { |definition| [definition.type, definition.arguments] },
      )
    end

    def import(records)
      @definitions = records.map do |type, arguments|
        Definition.new(type, arguments.freeze)
      end.freeze
      self
    end
  end
end
