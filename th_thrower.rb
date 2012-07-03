# -*- coding: utf-8 -*-
require "bundler"
Bundler.setup

require 'fileutils'
require 'json'

## setup thread
require 'thread'
MAX_THREAD = 10
# STOP queue
STOP = 0x01
threads = []
queue = Queue.new

require 'logger'
$log = Logger.new(STDOUT)

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

# @bits_root = "/var/db/riak/bitcask/"
@bits_root = "/Users/sawanoboriyu/github/local/bitcask_dumper/bits/bitcask/"

# setup riak client
@riak = Riak::Client.new(:host => '127.0.0.1', :protocol => "pbc")


MAX_THREAD.times do
  threads << Thread.start do
    $log.info "-- Stating thread --"

    # queue.popでenqueueされるまでwaitする。
    while q = queue.pop

      if q == STOP
        $log.info "-- Receive STOP Queue --"
        # 全てのthreadを止めるため、STOPを受け取ったらqueueに再度STOPを入れる
        queue << STOP
        break
      end
      
      ### load bitcask
      b = Bitcask.new File.join(@bits_root, q)
      b.load
      
      b.each do |key, value|
        next if value == Bitcask::TOMBSTONE
      
        bucket, key = BERT.decode key
        value = BERT.decode value
  
  
        # exclude expired data
        if bucket =~ /_net[0-9]$/ then
          if key.to_i / 1000 < C_EXPIRE
            # log to 
            $log.info "expired data : " + bucket + "/" + key
            next
          end
        end
  

        # check if exist before restore key-values.
        begin
          @riak.bucket[bucket][key]
        rescue Riak::HTTPFailedRequest
          # throw to riak
          $log.info "throw to riak : " + bucket + "/" + key
          begin
            ob = @riak.bucket(bucket)
            o = ob.get_or_new(key)
            o.raw_data = value.last
            o.content_type = "application/json"
            o.store
          rescue => e
            ## failed key name
            $log.error "store failed  : " + bucket + "/" + key
            ## logging backtrace
            $log.error e.exception
          end
        else
          ## store skiped
          $log.info "skip exist key: " + bucket + "/" + key
        end
      end
    end
  end
end


Dir.entries(@bits_root).each do |bit_dir|
  ### skip . and ..
  next if bit_dir =~ /^\./

  queue << bit_dir
end


queue << STOP

# 全てのthreadをmain threadに合流させる（終わるのを待つ）
threads.map(&:join)
