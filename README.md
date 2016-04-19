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
1) Using a configuration file passed as parameter on the command line.
2) Using individual configuration properties passed as parameters on the command line.
The command line parameters have precedence over the configuration file.

## Contributing

For now this is an internal Criteo project, but were aiming for making it open source and publishing to GitHub.

Issue reports and merge requests are welcome on Criteo's GitLab at : https://gitlab.criteois.com/ruby-gems/cassback

