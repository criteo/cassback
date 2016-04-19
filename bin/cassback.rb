#!/usr/bin/env ruby
require 'logger'
require 'optparse'
require 'yaml'

require_relative '../lib/hadoop.rb'
require_relative '../lib/cassandra.rb'
require_relative '../lib/backuptool.rb'

# This allows merging hashes that can contain themself hashes,
class ::Hash
  def deep_merge!(second)
    merger = proc { |_key, v1, v2| Hash === v1 && Hash === v2 ? v1.merge(v2, &merger) : Array === v1 && Array === v2 ? v1 | v2 : [:undefined, nil, :nil].include?(v2) ? v1 : v2 }
    merge!(second.to_h, &merger)
  end
end

# Create a Ruby logger with time/size rotation that logs both to file and console.
two_mb = 2 * 1024 * 1024
logger = Logger.new('| tee cassback.log', 'weekly', two_mb)

#  Default action
action = nil

# Default config file
config_file = ''

# Default command line config
command_line_config = {}

# Default options
options = {
  'cassandra' => {
    'config' => '/etc/cassandra/conf/cassandra.yaml',
  },
  'hadoop'    => {
    'hostname'  => 'localhost',
    'port'      => 14_000,
    'directory' => 'cassandra',
  },
  'restore'   => {
    'destination' => 'cassandra',
  },
}

# If no argument given in command line, print the help
ARGV << '-h' if ARGV.empty?

# Parse command line options
parser = OptionParser.new do |opts|
  opts.banner = 'Usage: cassback.rb [options]'

  opts.separator ''
  opts.separator 'Configuration:'
  opts.on('-C', '--config CONFIGFILE', 'Configuration file for the application') do |v|
    config_file = v
  end

  opts.separator ''
  opts.separator 'Actions:'
  opts.on('-S', '--snapshot', 'creates a new snapshot and send it to Hadoop') do |_v|
    action = 'new'
  end
  opts.on('-R', '--restore', 'restores a snapshot from Hadoop, needs a date and a destination') do |_v|
    action = 'restore'
  end
  opts.on('-L', '--list', 'list snapshots on Hadoop') do |_v|
    action = 'list'
  end
  opts.on('-F', '--flush', 'removes a backuped snapshot from Hadoop, needs a date') do |_v|
    action = 'delete'
  end

  opts.separator ''
  opts.separator 'Action related:'
  opts.on('-n', '--node NODE', 'Cassandra server node (default is current host)') do |v|
    options['node'] = v
  end
  opts.on('-d', '--date DATE', 'snapshot date, like YYYY_MM_DD') do |v|
    options['date'] = v
  end
  opts.on('-t', '--destination DIR', 'local destination path for restore (default is cassandra)') do |v|
    options['restore']['destination'] = v
  end

  opts.separator ''
  opts.separator 'Hadoop (WebHDFS):'
  opts.on('-H', '--host HOSTNAME', 'Hostname (default is localhost)') do |v|
    command_line_config['hadoop']['host'] = v
  end
  opts.on('-P', '--port PORT', 'Port (default is 14000)') do |v|
    command_line_config['hadoop']['port'] = v
  end
  opts.on('-D', '--directory DIRECTORY', 'Directory where to store backups (default is cassandra)') do |v|
    command_line_config['hadoop']['directory'] = v
  end

  opts.separator ''
  opts.separator 'Cassandra:'
  opts.on('-F', '--cassandra CONFIGFILE', 'Cassandra configuration file (default is /etc/cassandra/conf/cassandra.yaml)') do |v|
    command_line_config['cassandra']['config'] = v
  end

  opts.separator ''
  opts.separator 'Help:'
  opts.on('-h', '--help', 'Displays Help') do
    puts opts
    exit
  end
end
parser.parse!

# Read the configuration file if exist
begin
  options.deep_merge!(YAML.load_file(config_file))
  logger.info("Using configuration file #{config_file}")
rescue
  logger.warn('Unable to read configuration file, continue with default settings')
ensure
  # merge with command line settings.§
  options.deep_merge!command_line_config
end

# Fail if no action specified
if action.nil?
  logger.error('No action given')
  exit(1)
end

begin
  # Create the Hadoop object
  hadoop = Hadoop.new(host: options['hadoop']['hostname'], port: options['hadoop']['port'], base_dir: options['hadoop']['directory'])

  #  Create the Cassandra object
  cassandra = Cassandra.new(options['cassandra']['config'], logger)

  #  Create the backup object
  bck = BackupTool.new(cassandra, hadoop, logger)

  # If no node specified, use the local node
  options['node'] = cassandra.node_name unless options.include? 'node'

  #  New snapshot
  if action == 'new'
    bck.new_snapshot

  # Restore a snapshot
  elsif action == 'restore'
    raise('No date given') unless options.include? 'date'
    bck.restore_snapshot(options['node'], options['date'], options['restore']['destination'])

  # List snapshots
  elsif action == 'list'
    bck.list_snapshots(node: options['node'])

  #  Delete a snapshot
  elsif action == 'delete'
    raise('No date given') unless options.include? 'date'
    bck.delete_snapshots(node: options['node'], date: options['date'])
  end

#  In case of failure
rescue Exception => e
  logger.error(e.message)
  exit(1)
end

exit(0)
