require 'set'
require 'socket'
require 'yaml'

class Cassandra
  attr_reader :data_path, :cluster_name, :node_name

  def initialize(config_file, logger)
    @logger = logger

    read_config_file(config_file)

    @node_name = Socket.gethostname

    @logger.info("Cassandra cluster name = #{@cluster_name}")
    @logger.info("Cassandra node name = #{@node_name}")
    @logger.info("Cassandra data path = #{@data_path}")
  end

  def read_config_file(config_file)
    config = YAML.load_file(config_file)
    if config.include? 'cluster_name'
      @cluster_name = config['cluster_name'].tr(' ', '_')
    else
      @logger.warn("Could not found cluster name in Cassandra config file #{@config_file}")
      @cluster_name = 'noname_cassandra_cluster'
    end
    if config.include? 'data_file_directories'
      if config['data_file_directories'].length == 1
        @data_path = config['data_file_directories'][0]
      else
        # TODO : manage multiple data directories
        raise('This backup tool does not currently work with multiple data directories')
      end
    else
      raise('Not data directory defined in config file')
    end
  rescue Exception => e
    raise("Could not parse Cassandra config file #{config_file} (#{e.message})")
  end

  private :read_config_file

  def nodetool_snapshot(name)
    @logger.debug("Starting a new Cassandra snapshot #{name}")
    begin
      success = system('nodetool', 'snapshot', '-t', name)
      if success
        @logger.debug('Cassandra Snapshot successful')
      else
        raise
      end
    rescue Exception => e
      raise("Error while snapshot command (#{e.message})")
    end
  end

  private :nodetool_snapshot

  def nodetool_clearsnapshot(name)
    @logger.debug("Deleting snapshot #{name} in Cassandra")
    begin
      success = system('nodetool', 'clearsnapshot', '-t', name)
      if success
        @logger.debug('Cassandra Snapshot deletion successful')
      else
        raise
      end
    rescue Exception => e
      raise("Error while clearsnapshot command (#{e.message})")
    end
  end

  private :nodetool_clearsnapshot

  def get_keyspaces_and_tables
    result = {}
    Dir.foreach(@data_path) do |keyspace|
      next if keyspace == '.' || keyspace == '..'
      result[keyspace] = []
      Dir.foreach(@data_path + '/' + keyspace) do |table|
        next if table == '.' || table == '..'
        result[keyspace].push(table)
      end
    end
    result
  end

  private :get_keyspaces_and_tables

  def build_metadata(name)
    result = Set.new
    ks = get_keyspaces_and_tables
    ks.each do |keyspace, tables|
      tables.each do |table|
        snapdir = @data_path + '/' + keyspace + '/' + table + '/snapshots/' + name
        next unless Dir.exist?(snapdir)
        Dir.foreach(snapdir) do |filename|
          next if filename == '.' || filename == '..'
          result.add(keyspace + '/' + table + '/snapshots/' + name + '/' + filename)
        end
      end
    end
    result
  end

  private :build_metadata

  def new_snapshot
    today = Time.new.strftime('%Y_%m_%d')
    snapname = 'cass_snap_' + today

    nodetool_snapshot(snapname)
    metadata = build_metadata(snapname)

    CassandraSnapshot.new(@cluster_name, @node_name, today, metadata)
  end

  def delete_snapshot(snapshot)
    snapname = 'cass_snap_' + snapshot.date
    nodetool_clearsnapshot(snapname)
  end
end

class CassandraSnapshot
  attr_reader :cluster, :node, :date, :metadata

  def initialize(cluster, node, date, metadata = nil)
    @cluster = cluster
    @node = node
    @date = date
    @metadata = if metadata.nil?
                  Set.new
                else
                  metadata
    end
  end

  def to_s
    "[#{@cluster}|#{@node}|#{@date}]"
  end

  def ==(other)
    @cluster == other.cluster && @node == other.node && @date == other.date
  end

  def <=>(other)
    c = @cluster <=> other.cluster
    n = @node <=> other.node
    d = @date <=> other.date
    c * 3 + n * 2 + d
  end
end
