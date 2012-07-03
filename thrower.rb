# -*- coding: utf-8 -*-
require "bundler"
Bundler.setup

require 'fileutils'
require 'json'

### external
require 'bitcask'
require 'bert'
require 'riak'

# CONST
BASETIME = Time.now
BACK_DIR = "./dump/" + BASETIME.strftime("%Y-%m-%dT%H%M")

## define expire date
# > 60 * 60 * 24 * 60 #=> 5184000
EXPIRE = 5184000
C_EXPIRE = BASETIME.to_i - EXPIRE


### change STDOUT to logfile
if !ENV["DEBUG"] then
  File.open("./log/" + BASETIME.strftime("%Y-%m-%dT%H%M") + "_throw.log", "a") do |file|
    puts "STDOUT redirect to #{file.path}.."
    STDOUT.reopen(file)
  end
end

@bits_root = "/var/db/riak/bitcask/"
# @bits_root = "/Users/sawanoboriyu/github/local/bitcask_dumper/bits/bitcask/"

# setup riak client
@riak = Riak::Client.new(:host => '127.0.0.1', :protocol => "pbc")

def throw_to_riak()
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


      # exclude expired data
      if bucket =~ /_net[0-9]$/ then
        if key.to_i / 1000 < C_EXPIRE
          # log to 
          puts "expired data : " + bucket + "/" + key
          next
        end
      end

      # throw to riak
      puts "throw to riak: " + bucket + "/" + key
  
      begin
        ob = @riak.bucket(bucket)
        o = ob.get_or_new(key)
        o.raw_data = value.last
        o.content_type = "application/json"
        o.store
      rescue => e
        ## failed key name
        puts "store failed : " + bucket + "/" + key
        ## logging backtrace
        puts e.exception
      end    

    end
  end
end

throw_to_riak()
