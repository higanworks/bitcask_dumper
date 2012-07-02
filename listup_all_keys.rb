# -*- coding: utf-8 -*-
# list all keys to STDOUT.

require "bundler"
Bundler.setup

require 'fileutils'
require 'json'

### external
require 'bitcask'
require 'bert'
# require 'riak'

# CONST
# BASETIME = Time.now
# BACK_DIR = "./dump/" + BASETIME.strftime("%Y-%m-%dT%H%M")

### change STDOUT to logfile
# if !ENV["DEBUG"] then
#   File.open("./log/" + BASETIME.strftime("%Y-%m-%dT%H%M") + "_dumpout.log", "a") do |file|
#     STDOUT.reopen(file)
#   end
# end

# @bits_root = "/var/db/riak/bitcask/"
@bits_root = "/Users/sawanoboriyu/github/local/bitcask_dumper/bits/bitcask/"


Dir.entries(@bits_root).each do |bit_dir|
  ### skip . and ..
  next if bit_dir =~ /^\./

  ### load bitcask
  b = Bitcask.new File.join(@bits_root, bit_dir)
  b.load
  
  b.each do |key, value|
    next if value == Bitcask::TOMBSTONE
  
    bucket, key = BERT.decode key
    value = BERT.decode value
  
    puts bucket + "/" + key
  end
end
