#!/usr/bin/env ruby
require_relative '../lib/cassandra'

# Stub implementation that simulates cassandra backups.
class CassandraStub
  attr_reader :data_path, :cluster_name, :node_name

  def initialize(cluster_name = 'cluster1', node_name = 'node1', date = '', file_indexes = [])
    @cluster_name = cluster_name
    @node_name = node_name
    @date = date
    @data_path = 'test/cassandra' + '/' + cluster_name + '/' + node_name + '/'
    FileUtils.mkdir_p(@data_path)

    # create some fake sstables
    @metadata = Set.new
    file_indexes.each do |index|
      file_name = "SSTable-#{index}-Data.db"
      file_path = @data_path + '/' + file_name
      File.open(file_path, 'w') { |file| file.write('This is a test file that simulates an SSTable') }
      @metadata.add(file_name)
    end
  end

  def new_snapshot
    # simple create a pointer to an existing location
    CassandraSnapshot.new(@cluster_name, @node_name, @date, @metadata)
  end

  def delete_snapshot(_snapshot)
    FileUtils.rm_rf(@data_path)
  end
end
