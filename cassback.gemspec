# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'cassback/version'

Gem::Specification.new do |spec|
  spec.name          = "cassback"
  spec.version       = Cassback::VERSION
  spec.authors       = ["Vincent Van Hollebeke", "Bogdan Niculescu"]
  spec.email         = ["v.vanhollebeke@criteo.com", "b.niculescu@criteo.com"]

  spec.summary       = "Cassandra backup to HDFS."
  spec.description   = "This is a tool that allows creating backups of Cassandra and pushing them into HDFS."
  spec.homepage      = "https://gitlab.criteois.com/ruby-gems/cassback"

  # Prevent pushing this gem to RubyGems.org by setting 'allowed_push_host', or
  # delete this section to allow pushing this gem to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against public gem pushes."
  end

  # spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.files         = ['lib/hadoop.rb', 'lib/cassandra.rb', 'lib/backuptool.rb']
  spec.bindir        = "bin"
  spec.executables   << 'cassback.rb'
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.11"
  spec.add_development_dependency "rake", "~> 10.0"
end
