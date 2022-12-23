#! /usr/bin/env bash
# Installs RocksDB
# https://github.com/facebook/rocksdb/blob/master/INSTALL.md
# http://pyrocksdb.readthedocs.io/en/v0.4/installation.html
##

set -e
set -x

sudo yum groupinstall -y "Development Tools"
#sudo yum --enablerepo=epel install -y snappy{-devel,} zlib{-devel,} bzip2-{devel,libs} gflags{-devel,} lz4-devel libsanitizer libzstd{-devel,}

export CPATH=/usr/include
export LIBRARY_PATH=/usr/lib64
# Our versions of gcc is more strict by default, we need to disable that!
export EXTRA_CFLAGS='-O3 -Wimplicit-fallthrough=0 -Wformat-truncation=0'
export EXTRA_CXXFLAGS='-O3 -Wimplicit-fallthrough=0 -Wformat-truncation=0'
export DEBUG_LEVEL=0

declare rocksVersion="7.7.3"
wget -O rocksdb-${rocksVersion}.tar.gz https://github.com/facebook/rocksdb/archive/v${rocksVersion}.tar.gz
tar -xzf rocksdb-${rocksVersion}.tar.gz

pushd rocksdb-${rocksVersion}
make static_lib
sudo make install-static INSTALL_PATH=/usr/local


