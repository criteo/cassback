require 'webhdfs'
require 'webhdfs/fileutils'

WebHDFS::ClientV1::REDIRECTED_OPERATIONS.delete('OPEN')

class Hadoop < WebHDFS::Client
  attr_reader :base_dir

  def initialize(host: 'localhost', port: 14_000, base_dir: '/')
    super(host = host, port = port)
    @kerberos = true
    @base_dir = base_dir
  end
end
