# frozen_string_literal: true

source 'https://rubygems.org'

gemspec

group :development, :test do
  gem 'connection_pool'
  gem 'debug'
  gem 'minitest', '~> 5'
  gem 'opentelemetry-sdk'
  gem 'rack', '~> 2.0', '>= 2.2.0'
  gem 'rake', '~> 13.0'
  gem 'rubocop'
  gem 'rubocop-minitest'
  gem 'rubocop-performance'
  gem 'rubocop-rake'
  gem 'rubocop-thread_safety'
  gem 'simplecov'
  gem 'stackprof', platform: :mri
  gem 'toxiproxy'
end

group :test do
  if Gem::Version.new(RUBY_VERSION) >= Gem::Version.new('3.3')
    gem('async')
    gem('io-event', '~> 1.21.1')
  end
  gem 'ruby-prof', platform: :mri
end
