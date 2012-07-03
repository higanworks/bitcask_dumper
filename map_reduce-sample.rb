# -*- coding: utf-8 -*-
# map reduce for listup_all_keys.rb
require "bundler"
require "pp"
Bundler.setup

@ma = {}

# map
# bucket to hash_key
# keys to array into hash_value 
File.open("dump/list.txt") do |file|
  while line = file.gets
    # puts @ma[line.split("/")[0]].class
    key = line.split("/")[0]
    value = line.split("/")[1]
    @ma[key] = [] if !@ma.has_key?(key)
    @ma[key]<< value.chomp
  end
end


# reduce
# count: how much values does key have.
@ma.each do |k, v|
  count = 0
  v.each do |da|
    count = count + 1
  end
  puts k + "," + count.to_s
end