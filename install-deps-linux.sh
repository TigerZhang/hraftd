#!/usr/bin/env bash
OS=$(lsb_release -si)
ARCH=$(uname -m | sed 's/x86_//;s/i[3-6]86/32/')
VER=$(lsb_release -sr)

function install_gcc48 {
	sudo add-apt-repository ppa:ubuntu-toolchain-r/test
	sudo apt-get update; sudo apt-get install gcc-4.8 g++-4.8

	sudo update-alternatives --remove-all gcc 
	sudo update-alternatives --remove-all g++
	sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.8 20
	sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.8 20
	sudo update-alternatives --config gcc
	sudo update-alternatives --config g++
}

sudo apt-get install automake autoconf libtool
sudo apt-get install pkg-config
ROOTDIR=`pwd`
mkdir deps
git clone https://github.com/Cyan4973/lz4.git deps/lz4
cd deps/lz4
make
cd lib
make

cd "$ROOTDIR"
git clone https://github.com/google/snappy.git deps/snappy
cd deps/snappy
./autogen.sh
./configure --prefix=$ROOTDIR/deps/snappy/dist
make
make install

cd "$ROOTDIR"
install_gcc48

SNAPPY_DIR=/usr/local
LEVELDB_DIR=/usr/local/leveldb
ROCKSDB_DIR=$ROOTDIR/deps/rocksdb

CGO_CFLAGS=
CGO_CXXFLAGS=
CGO_LDFLAGS=
LD_LIBRARY_PATH=
DYLD_LIBRARY_PATH=
GO_BUILD_TAGS=

CGO_CFLAGS="$CGO_CFLAGS -I$ROCKSDB_DIR/include -I $ROOTDIR/deps/lz4/lib"
CGO_CXXFLAGS="$CGO_CXXFLAGS -I$ROCKSDB_DIR/include -I $ROOTDIR/deps/lz4/lib"
CGO_LDFLAGS="$CGO_LDFLAGS -L$ROCKSDB_DIR $ROOTDIR/deps/rocksdb/librocksdb.a $ROOTDIR/deps/snappy/dist/lib/libsnappy.a -lbz2 $ROOTDIR/deps/lz4/lib/liblz4.a -lz -lrt"
LD_LIBRARY_PATH=$(add_path $LD_LIBRARY_PATH $ROCKSDB_DIR)
DYLD_LIBRARY_PATH=$(add_path $DYLD_LIBRARY_PATH $ROCKSDB_DIR)
GO_BUILD_TAGS="$GO_BUILD_TAGS rocksdb"

export CGO_CFLAGS
export CGO_CXXFLAGS
export CGO_LDFLAGS
export LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH
export GO_BUILD_TAGS
