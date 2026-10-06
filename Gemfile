# frozen_string_literal: true

source "https://rubygems.org"

# Use sibling checkouts when developing locally; fall back to rubygems.org in CI.
%w[base-service callback-collection solid-redis solid-trace].each do |name|
  local = File.expand_path("../#{name}", __dir__)
  gem name, path: local if File.directory?(local)
end

gemspec
