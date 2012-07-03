# bitcask dumper for riak.

dump all buckets, keys and raw value data from riak backend storage.

## Usage

### setup

    bundle

#### bundle option for SmartOS64
    CONFIGURE_ARGS="--with-cflags='-m64' --with-ldflags='-m64'" bundle

### listup

    ruby ./listup_all_keys.rb
    

### dump
Dump all record to ./dump/ directory.

    ruby ./dumpall.rb



## Maintenance Smart Data Center's riak
reap past net* billingdata.

1. disable current riak.
2. mv bitcask directory.
3. enable riak(empty).
4. exec "ruby ./th_throw.rb"

### for thread version (th_throw.eb)
Recommend to increase nofiles before running.

    uname -n 30000    

## LICENSE
MIT. see LICENSE.txt