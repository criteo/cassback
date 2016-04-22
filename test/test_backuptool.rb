#!/usr/bin/env ruby
require 'test/unit'
require 'logger'

require_relative '../lib/backuptool'
require_relative 'hadoop_stub'
require_relative 'cassandra_stub'

class TestSimpleNumber < Test::Unit::TestCase
  def test_new_snapshot
    hadoop = HadoopStub.new('test/hadoop')
    create_new_snapshot(hadoop, 'node1', '2016_04_22', [1, 2])

    remote_files = hadoop.list('test/hadoop')
    # two files were backed up + one metadata file
    assert_equal(3, remote_files.size)

    # files were created in the correct location
    assert_equal('test/hadoop/cass_snap_metadata/cluster1/node1/cass_snap_2016_04_22', remote_files[0])
    assert_equal('test/hadoop/cluster1/node1/SSTable-1-Data.db', remote_files[1])
    assert_equal('test/hadoop/cluster1/node1/SSTable-2-Data.db', remote_files[2])

    # metadata file contains the sstables.
    metadata_content = File.open(remote_files[0], 'r').read
    assert(metadata_content.include? 'SSTable-1-Data.db')
    assert(metadata_content.include? 'SSTable-2-Data.db')

    # delete the hadoop folder
    hadoop.delete('test/hadoop')
    hadoop.delete('test/cassandra')
  end

  def test_two_snapshots
    hadoop = HadoopStub.new('test/hadoop')
    create_new_snapshot(hadoop, 'node1', '2016_04_22', [1, 2])
    create_new_snapshot(hadoop, 'node1', '2016_04_23', [2, 3, 4])

    remote_files = hadoop.list('test/hadoop')
    # two files were backed up + one metadata file
    assert_equal(6, remote_files.size)

    # files were created in the correct location
    # no duplicate files are stored
    assert_equal('test/hadoop/cass_snap_metadata/cluster1/node1/cass_snap_2016_04_22', remote_files[0])
    assert_equal('test/hadoop/cass_snap_metadata/cluster1/node1/cass_snap_2016_04_23', remote_files[1])
    assert_equal('test/hadoop/cluster1/node1/SSTable-1-Data.db', remote_files[2])
    assert_equal('test/hadoop/cluster1/node1/SSTable-2-Data.db', remote_files[3])
    assert_equal('test/hadoop/cluster1/node1/SSTable-3-Data.db', remote_files[4])
    assert_equal('test/hadoop/cluster1/node1/SSTable-4-Data.db', remote_files[5])

    # metadata on first backup file contains the sstables.
    metadata_content = File.open(remote_files[0], 'r').read
    assert(metadata_content.include? 'SSTable-1-Data.db')
    assert(metadata_content.include? 'SSTable-2-Data.db')

    # metadata on second backup file contains the sstables.
    metadata_content = File.open(remote_files[1], 'r').read
    assert(metadata_content.include? 'SSTable-2-Data.db')
    assert(metadata_content.include? 'SSTable-3-Data.db')
    assert(metadata_content.include? 'SSTable-4-Data.db')

    # cleanup
    hadoop.delete('test/hadoop')
    hadoop.delete('test/cassandra')
  end

  def test_restore
    hadoop = HadoopStub.new('test/hadoop')
    backup_tool = create_new_snapshot(hadoop, 'node1', '2016_04_22', [1, 2])

    # restore a newly created snapshot
    backup_tool.restore_snapshot('node1', '2016_04_22', 'test/restore')

    restored_files = hadoop.list('test/restore')
    # two files were restored
    assert_equal(2, restored_files.size)
    assert_equal('test/restore/SSTable-1-Data.db', restored_files[0])
    assert_equal('test/restore/SSTable-2-Data.db', restored_files[1])

    # cleanup
    hadoop.delete('test/hadoop')
    hadoop.delete('test/restore')
    hadoop.delete('test/cassandra')
  end

  def test_delete
    hadoop = HadoopStub.new('test/hadoop')
    backup_tool = create_new_snapshot(hadoop, 'node1', '2016_04_22', [1, 2])

    # delete a newly created snapshot
    backup_tool.delete_snapshots(node: 'node1', date: '2016_04_22')

    remote_files = hadoop.list('test/hadoop')
    assert_equal(0, remote_files.size)

    hadoop.delete('test/cassandra')
  end

  def create_new_snapshot(hadoop, node, date, file_indexes)
    logger = Logger.new(STDOUT)
    cassandra = CassandraStub.new('cluster1', node, date, file_indexes)
    backup_tool = BackupTool.new(cassandra, hadoop, logger)

    backup_tool.new_snapshot

    backup_tool
  end
end
