# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'cassback/version'

Gem::Specification.new do |spec|
  spec.name          = 'cassback'
  spec.version       = Cassback::VERSION
  spec.authors       = ['Vincent Van Hollebeke', 'Bogdan Niculescu']
  spec.email         = ['v.vanhollebeke@criteo.com', 'b.niculescu@criteo.com']

  spec.summary       = 'Cassandra backup to HDFS.'
  spec.description   = 'This is a tool that allows creating backups of Cassandra and pushing them into HDFS.'
  spec.homepage      = 'http://rubygems.org/gems/cassback'

  spec.licenses = ['Apache-2.0']

  spec.files         = `git ls-files`.split("\n")
  spec.test_files    = `git ls-files -- test/*`.split("\n")
  spec.bindir        = 'bin'
  spec.executables << 'cassback'
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler', '~> 1.11'
  spec.add_development_dependency 'rake', '~> 10.0'

  spec.add_runtime_dependency 'gssapi', '~> 1.2', '>= 1.2.0'
  spec.add_runtime_dependency 'webhdfs', '~> 0.8', '>= 0.8.0'
  spec.add_runtime_dependency 'table_print', '~> 1.5', '>= 1.5.6'
  spec.add_runtime_dependency 'filesize', '~> 0.1', '>= 0.1.1'
end
