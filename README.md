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

When the cassback gem installed it adds the **cassback** executable file into your PATH variable.
This means that you can execute it using one of the following commands and it will return example of usage :

    cassback
    cassback -h

A simple command that you can use for starting a backup is :

    cassback -S -C path_to_some_config_file.yml

## Configuration

The application has some default configuration defined.
You can overwrite the default configuration using two meanings :

1. Using a configuration file passed as parameter on the command line.

2. Using individual configuration properties passed as parameters on the command line.
The command line parameters have precedence over the configuration file.

## Orchestration

The tool is designed to do snapshots at **node level** (and not at **cluster level**) - basically it has to be installed
on each node and a separate process will have to be executed from there to trigger a node level snapshot. Because this task is
quite complex it is recommended to use an orchestration tool (like Rundeck) that allows you to execute same command
on multiple machines and run the processes in parallel.

After all node backups are finished the orchestration tool will have to take care of signaling other applications that
the backup is completely finished. That is done now by adding a new empty file on the cluster metadata folder that has
the format BACKUP_COMPLETED_yyyy_MM_dd. This has to be triggered only once by using the following command :

    cassback -B [-d date] -C conf/path_to_some_config_file.yml

Optionally you can also pass a date, if not present current day date will be assumed.

## Data Integrity

The project is using internally the webhdfs tool (see https://github.com/kzk/webhdfs)  that is a Ruby project
built on top of the WebHDFS API (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html).
Because we're using the WebHDFS API we get for free data integrity. The tool is also configurable so in case errors it
can retry the file download/upload of data. This is configurable via the following config file properties :

1. **hadoop.retryTimes** - the number of retries the tool should do before giving up. Default set to 5.
2. **hadoop.retryInterval** - the interval (in seconds) the tool should take between two attempts. Default set to 1 second.

If you want to check more about Hadoop's checksum algorithm that ensures data integrity you can check the
following link : https://www.safaribooksonline.com/library/view/hadoop-the-definitive/9781449328917/ch04.html

Also there is the **hadoop.readTimeout** property which has been set by default to 300s, but it can be configured to
another value if necessary (if HDFS cluster is responding too slow sometimes).

## Cleanup policy

Usually backups of databases take a lot of space. Even if we have optimized the code so the backups are done incrementally
(meaning that a file is not stored twice even if it's present in multiple backups), still cleanup needs to be done.
The tool has a cleanup policy of cleaning snapshots after some days have passed since the snapshot has been published.
This is configurable via the **cleanup.retentionDays** property in the configuration file. One point is that cleanup is
done at cluster level (for all nodes) since it doesn't make sense to keep data for only some of the nodes.

The command for triggering a cleanup is :

    cassback -A -C conf/path_to_some_config_file.yml

# Unit tests
Unit tests can be executed locally by running the following command :

    rake test

## Contributing

For now this is an internal Criteo project, but were aiming for making it open source and publishing to GitHub.

Issue reports and merge requests are welcome on Criteo's GitLab at : https://gitlab.criteois.com/ruby-gems/cassback

