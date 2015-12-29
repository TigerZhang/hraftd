_For background on this project check out this [blog post](http://www.philipotoole.com/building-a-distributed-key-value-store-using-raft/)._

hraftd [![Circle CI](https://circleci.com/gh/otoolep/hraftd/tree/master.svg?style=svg)](https://circleci.com/gh/otoolep/hraftd/tree/master) [![GoDoc](https://godoc.org/github.com/otoolep/hraftd?status.png)](https://godoc.org/github.com/otoolep/hraftd)
======

hraftd is a reference example use of the [Hashicorp Raft implementation](https://github.com/hashicorp/raft), inspired by [raftd](https://github.com/goraft/raftd). [Raft](https://raft.github.io/) is a _distributed consensus protocol_, meaning its purpose is to ensure that a set of nodes -- a cluster -- agree on the state of some arbitrary system, even when nodes are vulnerable to failure and network partitions. Distributed consensus is a fundamental concept when it comes to building fault-tolerant systems.

A simple example system like hraftd makes it easy to study Raft in general, and Hashicorp's implementation in particular.

## Reading and Writing Keys

Like raftd, the implementation is a very simple key-value store. You can set a key like so:

`curl -XPOST localhost:11000/key -d '{"foo": "bar"}'`

You can read the value for a key like so:

`curl -XGET localhost:11000/key/foo`

## Running hraftd
Starting and running a hraftd cluster is easy. Download hraftd like so:

```
mkdir hraftd
cd hraftd/
export GOPATH=$PWD
go get github.com/otoolep/hraftd
```

Run your first hraftd node like so:

`$GOPATH/bin/hraftd ~/node0`

You can now set a key and read its value back:

```
curl -XPOST localhost:11000/key -d '{"user1": "batman"}'
curl -XGET localhost:11000/key/user1
```

### Bring up a cluster
Let's bring up 2 more nodes, so we have a 3-node cluster. That way we can tolerate the failure of 1 node:

```
$GOPATH/bin/hraftd -haddr :11001 -raddr :12001 -join :11000 ~/node1
$GOPATH/bin/hraftd -haddr :11002 -raddr :12002 -join :11000 ~/node2
```

This tells each new node to join the existing node. Once joined, each node now knows about the key:

```
curl -XGET localhost:11000/key/user1
curl -XGET localhost:11001/key/user1
curl -XGET localhost:11002/key/user1
```

Furthermore you can add a second key:

`curl -XPOST localhost:11000/key -d '{"user2": "robin"}'`

Confirm that the new key has been set like so:

```
curl -XGET localhost:11000/key/user2
curl -XGET localhost:11001/key/user2
curl -XGET localhost:11002/key/user2
```

#### Stale reads
Because any node will answer a GET request, and nodes may be "fall behind" udpates, stale reads are possible. Again, hraftd is a simple program, for the purpose of demonstrating a consistent, distributed key-value store. These shortcomings can be addressed by enhancements to the existing source.

### Tolerating failure
Kill the leader process and watch one of the other nodes be elected leader. The keys are still available for query on the other nodes, and you can set keys on the new leader. Furthermore when the first node is restarted, it will rejoin the cluster and learn about any updates that occurred while it was down.

A 3-node cluster can tolerate the failure of a single node, but a 5-node cluster can tolerate the failure of two nodes. But 5-node clusters require that the leader contact more nodes before any change e.g. setting a key's value, can be considered committed.

### Leader-forwarding
Automatically forwarding requests to set keys to the current leader is not implemented. The client must always send requests to change a key to the leader or an error will be returned.

## Credits
Thanks to the authors of [raftd](https://github.com/goraft/raftd) for providing the inspiration for this system. The current use of Raft by [InfluxDB](https://github.com/influxdb/influxdb) was also helpful.

## Redis Support
FSM based on [ledis](https://github.com/siddontang/ledisdb).

```
redis-cli -p 6389 set key1 value1
redis-cli -p 6389 get key1
```

### Benchmark
```
MacBook Pro Retina 15' 2015 mid
2.5 GHz Intel Core i7
16 GB 1600 MHz DDR3
```

```
single node
✗ for c in 1 20 50 100; do echo "-c $c"; ~/redis-2.8.2/src/redis-benchmark -n 100000 -t set,get -c $c -p 6389 -q; done
-c 1
SET: 17556.18 requests per second
GET: 16747.61 requests per second

-c 20
SET: 47438.33 requests per second
GET: 43383.95 requests per second

-c 50
SET: 48146.37 requests per second
GET: 41736.23 requests per second

-c 100
SET: 47846.89 requests per second
GET: 40833.00 requests per second
```

### TODO
* snapshot support (DONE)

