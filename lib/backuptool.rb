require 'fileutils'
require 'table_print'
require 'filesize'

# Buffer size, used for downloads
BUFFER_SIZE = 10_000_000

# Directory where metadata is
META_DIR = 'cass_snap_metadata'.freeze

class BackupTool
  # Create a new BackupTool instance
  # * *Args*    :
  #   - +cassandra+ -> Cassandra instance
  #   - +hadoop+ -> HDFS instance
  #   - +logger+ -> Logger
  def initialize(cassandra, hadoop, logger)
    @cassandra = cassandra
    @hadoop = hadoop
    @logger = logger

    @metadir = META_DIR
  end

  def metadata_dir_for_backup(node, date)
      return metadata_dir() + node + '/cass_snap_' + date
  end

  def metadata_dir_for_node(node)
      return metadata_dir() + node + '/'
  end

  def metadata_dir()
      return @hadoop.base_dir + '/' + @metadir + '/' + @cassandra.cluster_name + '/'
  end

  def data_dir_for_node(node)
      return @hadoop.base_dir + '/' +  @cassandra.cluster_name + '/' + node + '/'
  end

  # Look for snapshots
  # * *Args*    :
  #   - +node+ -> Cassandra node name
  #   - +date+ -> HDFS instance
  def search_snapshots(node: 'ALL', date: 'ALL')

    # Look for all snapshots already existing for "node" at time "date"
    def get_snapshots_node(node, date)
      results = []
      dates = [date]
      begin
        if date == 'ALL'
          dates = @hadoop.list(metadata_dir_for_node(node))
                         .select { |dir| dir['pathSuffix'].include? 'cass_snap_' }
                         .map { |dir| dir['pathSuffix'].gsub('cass_snap_', '')}
        end

        dates.each do |date|
          metadata = @hadoop.read(metadata_dir_for_backup(node, date)).split("\n").to_set
          results.push(CassandraSnapshot.new(@cassandra.cluster_name, node, date, metadata))
        end

      rescue Exception => e
        @logger.warn("Could not get snapshots for node #{node} : #{e.message}")
      end

      return results
    end

    # Get the list of nodes
    def get_node_list(node)
      if node != 'ALL'
        return [node]
      end

      nodes = []
      begin
        nodes = @hadoop.list(metadata_dir())
                       .select { |item| item['type'].casecmp('DIRECTORY') == 0 }
                       .map { |item| item['pathSuffix'] }
                       .flatten
      rescue Exception => e
        @logger.warn("Could not get node list for cluster #{@cassandra.cluster_name} : #{e.message}")
      end

      return nodes
    end


    # RUN
    @logger.info("Searching snapshots for #{node} at time #{date}")
    snapshots = get_node_list(node).map { |node| get_snapshots_node(node, date) }
                                   .flatten
                                   .sort
    @logger.info("Found #{snapshots.length} snapshots")
    return snapshots
  end

  def list_snapshots(node: @cassandra.node_name)
    @logger.info('Listing available snapshots')
    snapshots = search_snapshots(node: node)
    tp(snapshots, 'cluster', 'node', 'date')
  end

  def prepare_hdfs_dirs(node)
    @logger.info(':::::::: Prepare HDFS ::::::::')
    begin
      paths = [data_dir_for_node(node), metadata_dir_for_node(node)]
      paths.each do |path|
        @logger.info("Creating destination directory " + path)
        if not @hadoop.mkdir(path)
          raise
        end
      end
    rescue Exception => e
        raise("Could not create your cluster directory : #{e.message}")
    end
  end

  def new_snapshot
    @logger.info(':::::::: Creating new snapshot ::::::::')
    snapshot = @cassandra.new_snapshot

    prepare_hdfs_dirs(snapshot.node)

    @logger.info(':::::::: Get last backup ::::::::')
    existing = search_snapshots(node: snapshot.node)
    last = if existing.empty?
           then CassandraSnapshot.new(snapshot.cluster, snapshot.node, 'never')
           else existing[-1] end
    @logger.info("Last snapshot is #{last}")
    files = snapshot.metadata - last.metadata
    @logger.info("#{files.length} files to upload")


    @logger.info('::::::: Uploading tables to HDFS ::::::')
    index = 0
    number_of_files = files.length
    total_file_size = 0
    files.each do |file|
      index += 1
      local = @cassandra.data_path + '/' + file
      local_file_size = File.size(local)
      total_file_size += local_file_size
      pretty_size = Filesize.from("#{local_file_size} B").pretty
      @logger.info("Sending file #{index}/#{number_of_files} #{file} having size #{pretty_size} to HDFS")
      remote = data_dir_for_node(snapshot.node) + file
      @logger.debug("#{local} => #{remote}")
      File.open(local, 'r') do |f|
        begin
          retries = 3
          @hadoop.create(remote, f, overwrite: true)
        rescue Exception => e
          @logger.info("HDFS write failed: #{e.message}")
          @logger.info("HDFS write retrying in 1s")
          sleep 1
          retry if (retries -= 1) < 0
        end
      end
    end

    total_file_size_pretty = Filesize.from("#{total_file_size} B").pretty
    @logger.info("Total size of uploaded files is #{total_file_size_pretty}")

    @logger.info('Sending metadata to HDFS')
    remote = metadata_dir_for_backup(snapshot.node, snapshot.date)
    @logger.debug("metadata => #{remote}")
    @hadoop.create(remote, snapshot.metadata.to_a * "\n", overwrite: true)

    @cassandra.delete_snapshot(snapshot)
    @logger.info('Success !')
  end

  def delete_snapshots(node: @cassandra.node_name, date: 'ALL')
    snapshots = search_snapshots(node: node, date: date)
    if snapshots.empty?
      raise('No snapshot found for deletion')
    end

    snapshots.each do |snapshot|
      @logger.info("Deleting snapshot #{snapshot}")
      node_snapshots = search_snapshots(node: snapshot.node)
      merged_metadata = Set.new
      node_snapshots.each do |s|
        merged_metadata += s.metadata if s != snapshot
      end
      files = snapshot.metadata - merged_metadata
      @logger.info("#{files.length} files to delete")
      files.each do |file|
        @logger.info("Deleting file #{file}")
        remote = data_dir_for_node(snapshot.node) + '/' + file
        @logger.debug("DELETE => #{remote}")
        @hadoop.delete(remote)
      end
      @logger.info('Deleting metadata in HDFS')
      remote = metadata_dir_for_backup(snapshot.node, snapshot.date)
      @logger.debug("DELETE => #{remote}")
      @hadoop.delete(remote)
    end
  end

  # Cleans up backups that are older than a number of days.
  # This functions cleans data on all nodes.
  def cleanup(days)
    retention_date = Date.today - days
    @logger.info("Cleaning backup data on all nodes before #{retention_date}.")

    all_snapshots = search_snapshots
    @logger.info("A total of #{all_snapshots.size} snapshots were found on HDFS.")

    snapshots_to_be_deleted = all_snapshots.select { |snapshot| snapshot.get_date < retention_date }
    @logger.info("A total of #{snapshots_to_be_deleted.size} snapshots will be deleted.")

    snapshots_to_be_deleted.each do |snapshot|
      delete_snapshots(node: snapshot.node, date: snapshot.date)
    end

    all_backup_flags = get_backup_flags
    @logger.info("A total of #{all_backup_flags.size} back up flags were found on HDFS.")

    backup_flags_to_be_delete = all_backup_flags.select { |flag| flag.date < retention_date }
    @logger.info("A total of #{backup_flags_to_be_delete.size} backup flags will be deleted.")

    backup_flags_to_be_delete.each do |flag|
      begin
        file = metadata_dir() + flag.file
        @logger.info("Deleting #{file}")
        @hadoop.delete(file)
      rescue Exception => e
        @logger.warn("Cannot delete #{file} reason: #{e.message}")
      end
    end
  end

  # Method that creates a backup flag to signal that the backup is finished on all nodes
  # This is an individual command that has to be called manually after snapshots have finished
  def create_backup_flag(date)
    file_name = 'BACKUP_COMPLETED_' + date
    remote_file = metadata_dir() + file_name

    @logger.info('Setting backup completed flag : ' + remote_file)
    @hadoop.create(remote_file, '', overwrite: true)
  end

  def get_backup_flags
    @hadoop.list(metadata_dir())
           .select { |item| item['pathSuffix'].include? 'BACKUP_COMPLETED_' }
           .collect { |file| BackupFlag.new(@cassandra.cluster_name, file['pathSuffix']) }
  end

  # Download a file from HDFS, buffered way
  # * *Args*    :
  #   - +remote+ -> HDFS path
  #   - +local+ -> local path
  def buffered_download(remote, local)
    @logger.debug("#{remote} => #{local}")

    # Create the destination directory if not exists
    path = File.dirname(local)
    FileUtils.mkdir_p(path) unless File.exist?(path)

    file = open(local, 'wb')

    offset = 0
    length = BUFFER_SIZE
    print '['
    while length == BUFFER_SIZE
      print '#'
      content = @hadoop.read(remote, offset: offset, length: BUFFER_SIZE)
      file.write(content)
      length = content.length
      offset += length
    end
    print "]\n"

    file.close
  end

  # Restore a snapshot from HDFS
  # * *Args*    :
  #   - +node+ -> node where the snapshot comes from
  #   - +date+ -> snapshot date
  #   - +destination+ -> local directory where to restore
  def restore_snapshot(node, date, destination, keyspace: 'ALL', table: 'ALL')
    # Search the snapshot matching node and date
    snapshots = search_snapshots(node: node, date: date)

    if snapshots.empty?
      raise('No snapshot found for restore')
    elsif snapshots.length > 1
      raise('More than one candidate snapshot to restore')
    else
      snapshot = snapshots[0]
      @logger.info("Restoring snapshot #{snapshot}")
      @logger.info("Snapshot has #{snapshot.metadata.length} files")

      files_to_be_restored = snapshot.metadata.select { |item|
        filename = File.basename(item)
        matches_keyspace = keyspace == 'ALL' || (filename.include? keyspace)
        matches_table =  table == 'ALL' || (filename.include? table)
        matches_keyspace && matches_table
      }

      @logger.info("Found #{files_to_be_restored.length} to be restored that match
              keyspace #{keyspace} and table #{table}")

      # For each file in the list
      files_to_be_restored.each do |file|
        @logger.info("Restoring file #{file}")
        local = destination + '/' + file
        remote = data_dir_for_node(snapshot.node) + file
        # Download the file from hdfs
        buffered_download(remote, local)
      end
      @logger.info('Success !')
    end
  end
end
