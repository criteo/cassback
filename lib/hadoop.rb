require 'webhdfs'
require 'webhdfs/fileutils'

WebHDFS::ClientV1::REDIRECTED_OPERATIONS.delete('OPEN')

class Hadoop < WebHDFS::Client
  attr_reader :base_dir

  def initialize(host: 'localhost', port: 14_000, base_dir: '/', retry_times: 5, retry_interval: 1)
    super(host = host, port = port)
    @kerberos = true
    @base_dir = base_dir
    @retry_known_errors = true
    @retry_times = retry_times
    @retry_interval = retry_interval
  end
end
