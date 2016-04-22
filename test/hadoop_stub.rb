#!/usr/bin/env ruby

require 'fileutils'

# A stub implementation of Hadoop that read/writes to local file instead of using webhdfs
class HadoopStub
  attr_reader :base_dir

  def initialize(base_dir)
    @base_dir = base_dir
  end

  def list(path, _options = {})
    filesAndFolders = Dir.glob("#{path}/**/*")
    filesAndFolders.collect do |file|
      { 'pathSuffix' => file }
    end
    filesAndFolders.select { |file| File.file?file }
  end

  def create(path, body, _options = {})
    parent = File.expand_path('..', path)
    FileUtils.mkdir_p parent
    if body.is_a?(File)
      File.open(path, 'w') { |file| file.write(body.read) }
    elsif
      File.open(path, 'w') { |file| file.write(body) }
    end
  end

  def read(path, _options = {})
    File.open(path, 'r').read
  end

  def delete(path, _options = {})
    FileUtils.rm_rf(path)
  end
end
