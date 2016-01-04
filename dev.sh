#!/bin/bash

#default snappy and leveldb install path
#you may change yourself
HOMEBREW_PREFIX=$(type brew >/dev/null 2>&1 && brew --prefix 2>/dev/null)
if [[ ! -z "$HOMEBREW_PREFIX" ]]; then
  SNAPPY_DIR=$HOMEBREW_PREFIX/opt/snappy
  LEVELDB_DIR=$HOMEBREW_PREFIX/opt/leveldb
  ROCKSDB_DIR=$HOMEBREW_PREFIX/opt/rocksdb
  LUA_DIR=$HOMEBREW_PREFIX/opt/lua51
fi
SNAPPY_DIR=/usr/local
LEVELDB_DIR=/usr/local/leveldb
ROCKSDB_DIR=/usr/local/Cellar/rocksdb/4.1/
LUA_DIR=/usr/local/lua

function add_path()
{
  # $1 path variable
  # $2 path to add
  if [ -d "$2" ] && [[ ":$1:" != *":$2:"* ]]; then
    echo "$1:$2"
  else
    echo "$1"
  fi
}

export GOPATH=$(add_path $GOPATH $LEDISROOT)

GO_BUILD_TAGS=
CGO_CFLAGS=
CGO_CXXFLAGS=
CGO_LDFLAGS=

# check dependent libray, now we only check simply, maybe later add proper checking way.

# check snappy 
if [ -f $SNAPPY_DIR/include/snappy.h ]; then
    CGO_CFLAGS="$CGO_CFLAGS -I$SNAPPY_DIR/include"
    CGO_CXXFLAGS="$CGO_CXXFLAGS -I$SNAPPY_DIR/include"
    CGO_LDFLAGS="$CGO_LDFLAGS -L$SNAPPY_DIR/lib -lsnappy"
    LD_LIBRARY_PATH=$(add_path $LD_LIBRARY_PATH $SNAPPY_DIR/lib)
    DYLD_LIBRARY_PATH=$(add_path $DYLD_LIBRARY_PATH $SNAPPY_DIR/lib)
fi

# check leveldb
if [ -f $LEVELDB_DIR/include/leveldb/c.h ]; then
    CGO_CFLAGS="$CGO_CFLAGS -I$LEVELDB_DIR/include"
    CGO_CXXFLAGS="$CGO_CXXFLAGS -I$LEVELDB_DIR/include"
    CGO_LDFLAGS="$CGO_LDFLAGS -L$LEVELDB_DIR/lib -lleveldb"
    LD_LIBRARY_PATH=$(add_path $LD_LIBRARY_PATH $LEVELDB_DIR/lib)
    DYLD_LIBRARY_PATH=$(add_path $DYLD_LIBRARY_PATH $LEVELDB_DIR/lib)
    GO_BUILD_TAGS="$GO_BUILD_TAGS leveldb"
fi

# check rocksdb
if [ -f $ROCKSDB_DIR/include/rocksdb/c.h ]; then
    CGO_CFLAGS="$CGO_CFLAGS -I$ROCKSDB_DIR/include -I /usr/local/Cellar/lz4/r131 -I /usr/local/Cellar/zlib/1.2.8"
    CGO_CXXFLAGS="$CGO_CXXFLAGS -I$ROCKSDB_DIR/include -I /usr/local/Cellar/lz4/r131 -I /usr/local/Cellar/zlib/1.2.8"
    CGO_LDFLAGS="$CGO_LDFLAGS -L$ROCKSDB_DIR/lib -lrocksdb -llz4 /usr/local/lib/libbz2.a -lz"
    LD_LIBRARY_PATH=$(add_path $LD_LIBRARY_PATH $ROCKSDB_DIR/lib /usr/local/Cellar/lz4/r131/lib /usr/local/Cellar/zlib/1.2.8/lib)
    DYLD_LIBRARY_PATH=$(add_path $DYLD_LIBRARY_PATH $ROCKSDB_DIR/lib /usr/local/Cellar/lz4/r131/lib /usr/local/Cellar/zlib/1.2.8/lib)
    GO_BUILD_TAGS="$GO_BUILD_TAGS rocksdb"
fi


#check lua
if [ -f $LUA_DIR/include/lua.h ]; then
    CGO_CFLAGS="$CGO_CFLAGS -I$LUA_DIR/include"
    CGO_LDFLAGS="$CGO_LDFLAGS -L$LUA_DIR/lib -llua"
    LD_LIBRARY_PATH=$(add_path $LD_LIBRARY_PATH $LUA_DIR/lib)
    DYLD_LIBRARY_PATH=$(add_path $DYLD_LIBRARY_PATH $LUA_DIR/lib)
    GO_BUILD_TAGS="$GO_BUILD_TAGS lua"
elif [ -f $LUA_DIR/include/lua-5.1/lua.h ]; then
    CGO_CFLAGS="$CGO_CFLAGS -I$LUA_DIR/include/lua-5.1"
    CGO_LDFLAGS="$CGO_LDFLAGS -L$LUA_DIR/lib -llua5.1"
    LD_LIBRARY_PATH=$(add_path $LD_LIBRARY_PATH $LUA_DIR/lib)
    DYLD_LIBRARY_PATH=$(add_path $DYLD_LIBRARY_PATH $LUA_DIR/lib)
    GO_BUILD_TAGS="$GO_BUILD_TAGS lua"
fi

export CGO_CFLAGS
export CGO_CXXFLAGS
export CGO_LDFLAGS
export LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH
export GO_BUILD_TAGS
