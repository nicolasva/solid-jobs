# frozen_string_literal: true

require_relative "lib/solid_jobs/version"

Gem::Specification.new do |spec|
  spec.name = "solid-jobs"
  spec.version = SolidJobs::VERSION
  spec.authors = ["Nicolas Vandenbogaerde"]

  spec.summary = "A Ractor-oriented Redis background job system"
  spec.description = "A Ractor-local Redis task runtime with its own envelope, channels, claims, interceptors, scheduling, and failure handling."
  spec.homepage = "https://github.com/nicolasva/solid-jobs"
  spec.license = "LGPL-3.0-or-later"
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
  spec.add_dependency "solid-redis", "~> 1.0", ">= 1.0.12"

  spec.add_development_dependency "minitest", ">= 5", "< 7"
  spec.add_development_dependency "rake", "~> 13.0"
end
