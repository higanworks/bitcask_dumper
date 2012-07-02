# -*- coding: utf-8 -*-
## This is thread version. very fast.
## But it's danger because riak has three same keys for redundancy.
## 

require "bundler"
Bundler.setup

require 'fileutils'
require 'json'

## setup thread
require 'thread'
MAX_THREAD = 5
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

### change STDOUT to logfile
if !ENV["DEBUG"] then
  File.open("./log/" + BASETIME.strftime("%Y-%m-%dT%H%M") + "_dumpout.log", "a") do |file|
    puts "STDOUT redirect to #{file.path}.."
    STDOUT.reopen(file)
  end
end

# @bits_root = "/var/db/riak/bitcask/"
@bits_root = "/Users/sawanoboriyu/github/local/bitcask_dumper/bits/bitcask/"



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
      
        # dump the entire value to a file for later inspection.
        FileUtils.mkdir_p(File.join(BACK_DIR,bucket))
        File.open(File.join(BACK_DIR, bucket, key), 'w') do |out|
          out.write value.to_json
        end

        $log.info bucket + "/" + key



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
