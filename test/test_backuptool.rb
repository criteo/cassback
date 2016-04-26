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

    remote_files = hadoop.list_files('test/hadoop')
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

    # cleanup
    hadoop.delete('test/hadoop')
    hadoop.delete('test/cassandra')
  end

  def test_two_snapshots
    hadoop = HadoopStub.new('test/hadoop')
    create_new_snapshot(hadoop, 'node1', '2016_04_22', [1, 2])
    create_new_snapshot(hadoop, 'node1', '2016_04_23', [2, 3, 4])

    remote_files = hadoop.list_files('test/hadoop')
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

    restored_files = hadoop.list_files('test/restore')
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

    remote_files = hadoop.list_files('test/hadoop')
    assert_equal(0, remote_files.size)

    hadoop.delete('test/cassandra')
  end

  def test_backup_flag
    hadoop = HadoopStub.new('test/hadoop')
    backup_tool = create_new_snapshot(hadoop, 'node1', '2016_04_22', [1, 2])

    backup_tool.create_backup_flag('2016_04_22')

    remote_files = hadoop.list_files('test/hadoop')
    assert_equal(4, remote_files.size)
    # Flag is created at cluster level
    assert_equal('test/hadoop/cass_snap_metadata/cluster1/BACKUP_COMPLETED_2016_04_22', remote_files[0])

    # cleanup
    hadoop.delete('test/hadoop')
    hadoop.delete('test/cassandra')
  end

  def test_get_backup_flag
    hadoop = HadoopStub.new('test/hadoop')
    backup_tool = create_new_snapshot(hadoop, 'node1', '2016_04_22', [1, 2])

    backup_tool.create_backup_flag('2016_04_22')
    flags = backup_tool.get_backup_flags

    # One flag found
    assert_equal(1, flags.size)
    # Flag points to the correct file
    assert_equal('cluster1', flags[0].cluster)
    assert_equal('BACKUP_COMPLETED_2016_04_22', flags[0].file)

    # cleanup
    hadoop.delete('test/hadoop')
    hadoop.delete('test/cassandra')
  end

  def test_cleanup
    hadoop = HadoopStub.new('test/hadoop')
    retention_days = 30

    date_31_days_back = (Date.today - 31).strftime('%Y_%m_%d')
    date_30_days_back = (Date.today - 30).strftime('%Y_%m_%d')

    # Two backups on two nodes
    create_new_snapshot(hadoop, 'node1', date_31_days_back, [1, 2, 3, 4])
    create_new_snapshot(hadoop, 'node2', date_31_days_back, [1, 2, 3, 4])
    create_new_snapshot(hadoop, 'node1', date_30_days_back, [3, 4, 5, 6])
    backup_tool = create_new_snapshot(hadoop, 'node2', date_30_days_back, [4, 5, 6, 7])

    # Both backups are marked as completed
    backup_tool.create_backup_flag(date_31_days_back)
    backup_tool.create_backup_flag(date_30_days_back)
    backup_tool.create_backup_flag(date_30_days_back)

    backup_tool.cleanup(retention_days)

    # Two snapshots were deleted, two were kept
    snapshots = backup_tool.search_snapshots
    assert_equal(2, snapshots.size)
    assert_equal('node1', snapshots[0].node)
    assert_equal(date_30_days_back, snapshots[0].date)
    assert_equal('node2', snapshots[1].node)
    assert_equal(date_30_days_back, snapshots[1].date)

    # One backup flag was deleted, one was kept.
    backup_flags = backup_tool.get_backup_flags
    assert_equal(1, backup_flags.size)
    assert_equal("BACKUP_COMPLETED_#{date_30_days_back}", backup_flags[0].file)

    # cleanup
    hadoop.delete('test/hadoop')
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
