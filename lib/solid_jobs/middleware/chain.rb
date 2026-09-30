# frozen_string_literal: true

module SolidJobs
  module Middleware
    class Chain
      Entry = Struct.new(:klass, :args, keyword_init: true)

      include Enumerable

      def initialize(copy = nil)
        @entries = copy ? copy.map { |entry| Entry.new(klass: entry.klass, args: entry.args.dup) } : []
      end

      def each(&block)
        @entries.each(&block)
      end

      def add(klass, *args)
        remove(klass)
        @entries << Entry.new(klass: klass, args: args)
        self
      end

      def prepend(klass, *args)
        remove(klass)
        @entries.unshift(Entry.new(klass: klass, args: args))
        self
      end

      def remove(klass)
        @entries.delete_if { |entry| entry.klass == klass }
        self
      end

      def insert_before(oldklass, newklass, *args)
        insert_relative(oldklass, newklass, args, 0)
      end

      def insert_after(oldklass, newklass, *args)
        insert_relative(oldklass, newklass, args, 1)
      end

      def exists?(klass)
        @entries.any? { |entry| entry.klass == klass }
      end
      alias_method :include?, :exists?

      def clear
        @entries.clear
        self
      end

      def empty?
        @entries.empty?
      end

      def retrieve
        @entries.map { |entry| entry.klass.new(*entry.args) }
      end

      def snapshot
        Utilities.shareable_copy(
          @entries.map { |entry| [entry.klass, entry.args] },
        )
      end

      def restore(snapshot)
        clear
        snapshot.each { |klass, args| add(klass, *args) }
        self
      end

      def invoke(*arguments, &final)
        return yield if empty?

        stack = retrieve
        traverse(stack, 0, arguments, &final)
      end

      def dup
        self.class.new(@entries)
      end

      private

      def traverse(stack, index, arguments, &final)
        return final.call if index >= stack.length

        stack[index].call(*arguments) do
          traverse(stack, index + 1, arguments, &final)
        end
      end

      def insert_relative(oldklass, newklass, args, offset)
        remove(newklass)
        index = @entries.index { |entry| entry.klass == oldklass }
        raise ArgumentError, "#{oldklass} is not in the middleware chain" unless index

        @entries.insert(index + offset, Entry.new(klass: newklass, args: args))
        self
      end
    end
  end
end
