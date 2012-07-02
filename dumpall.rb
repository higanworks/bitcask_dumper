# -*- coding: utf-8 -*-
require "bundler"
Bundler.setup

require 'fileutils'
require 'json'

### external
require 'bitcask'
require 'bert'
require 'riak'


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
    # dump the entire value to a file for later inspection.
    # FileUtils.mkdir_p(File.join(BACK_DIR,bucket))
    # File.open(File.join(BACK_DIR, bucket, key), 'w') do |out|
    #   out.write value.to_json
    # end
  end
end