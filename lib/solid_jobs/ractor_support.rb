# frozen_string_literal: true

module SolidJobs
  module RactorSupport
    module_function

    def value(ractor)
      ractor.respond_to?(:value) ? ractor.value : ractor.take
    end
  end
end
