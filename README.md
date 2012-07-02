# bitcask dumper for riak.

dump all buckets, keys and raw value data from riak backend storage.

## Usage

### dump
Dump all record to ./dump/ directory.

    bundle
    ruby ./dumpall.rb


#### bundle option for SmartOS64
    CONFIGURE_ARGS="--with-cflags='-m64' --with-ldflags='-m64'" bundle
