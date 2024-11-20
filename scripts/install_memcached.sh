#!/bin/bash

version=$MEMCACHED_VERSION


sudo apt-get -y remove memcached
sudo apt-get install libevent-dev

echo Installing Memcached version ${version}

# Install memcached TLS support
wget https://memcached.org/files/memcached-${version}.tar.gz
tar -zxvf memcached-${version}.tar.gz
cd memcached-${version}
./configure --enable-tls
make
sudo mv memcached /usr/local/bin/

echo Memcached version ${version} installation complete
