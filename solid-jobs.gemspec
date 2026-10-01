# frozen_string_literal: true

require_relative "lib/solid_jobs/version"

Gem::Specification.new do |spec|
  spec.name = "solid-jobs"
  spec.version = SolidJobs::VERSION
  spec.authors = ["Nicolas Vandenbogaerde"]

  spec.summary = "A Ractor-oriented Redis background job system"
  spec.description = "Ractor-local job clients and workers with Sidekiq-compatible Redis payloads, queues, scheduling, and retries."
  spec.homepage = "https://github.com/nicolasva/solid-jobs"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.2"

  spec.files = Dir["exe/*", "lib/**/*.rb", "docs/**/*.md", "README.md", "CHANGELOG.md", "LICENSE.txt"]
  spec.bindir = "exe"
  spec.executables = ["solid-jobs"]
  spec.require_paths = ["lib"]

  spec.metadata["rubygems_mfa_required"] = "true"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"

  spec.add_dependency "base-service", "~> 0.1"
  spec.add_dependency "callback-collection", "~> 0.2"
  spec.add_dependency "logger", ">= 1.7"
  spec.add_dependency "solid-redis", "~> 1.0", ">= 1.0.11"

  spec.add_development_dependency "minitest", ">= 5", "< 7"
  spec.add_development_dependency "rake", "~> 13.0"
end
