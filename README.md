# Cassback

Welcome to your Cassback!
This is a project that aims backup Cassandra SSTables and load them into HDFS for further usage.

## Installation

Build the application into a gem using the command

    $ gem build cassback.gemspec

You should the following output :

      Successfully built RubyGem
      Name: cassback
      Version: 0.1.0
      File: cassback-0.1.0.gem


Install the application into your local gem store using the following command :

    $ gem install cassback-0.1.0.gem

You should then see the following output :

    Successfully installed cassback-0.1.0
    Parsing documentation for cassback-0.1.0
    Done installing documentation for cassback after 0 seconds
    1 gem installed

## Usage

When the cassback gem installed it adds the **cassback.rb** file into your PATH variable.
This means that you can execute it using one of the following commands and it will return example of usage :

    cassback.rb
    cassback.rb -h

A simple command that you can use for starting a backup is :

    cassback.rb -S -C path_to_some_config_file.yml

## Configuration

The application has some default configuration defined.
You can overwrite the default configuration using two meanings :

1. Using a configuration file passed as parameter on the command line.

2. Using individual configuration properties passed as parameters on the command line.
The command line parameters have precedence over the configuration file.

## Data Integrity

The project is using internally the webhdfs tool (see https://github.com/kzk/webhdfs)  that is a Ruby project
built on top of the WebHDFS API (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html).
Because we're using the WebHDFS API we get for free data integrity. The tool is also configurable so in case errors it
can retry the file download/upload of data. This is configurable via the following config file properties :

1. **hadoop.retryTimes** - the number of retries the tool should do before giving up. Default set to 5.
2. **hadoop.retryInterval** - the interval (in seconds) the tool should take between two attempts. Default set to 1 second.

If you want to check more about Hadoop's checksum algorithm that ensures data integrity you can check the
following link : https://www.safaribooksonline.com/library/view/hadoop-the-definitive/9781449328917/ch04.html


## Contributing

For now this is an internal Criteo project, but were aiming for making it open source and publishing to GitHub.

Issue reports and merge requests are welcome on Criteo's GitLab at : https://gitlab.criteois.com/ruby-gems/cassback

