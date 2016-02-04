// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
	"strings"
	"strconv"
//	"encoding/binary"
//	"bufio"
	"math"
	"errors"

	"github.com/TigerZhang/raft"
//	"github.com/hashicorp/raft-boltdb"
	"github.com/TigerZhang/raft-leveldb"
	"github.com/TigerZhang/ledisdb/ledis"
	ledisstore "github.com/TigerZhang/ledisdb/store"
	lediscfg "github.com/TigerZhang/ledisdb/config"
//	"github.com/siddontang/go/snappy"
	"github.com/siddontang/go/hack"
	"encoding/binary"

	"github.com/garyburd/redigo/redis"

	"github.com/TigerZhang/hraftd/redislogstore"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 3 * time.Second
)

var (
	ErrEmptyCommand          = errors.New("empty command")
	ErrNotFound              = errors.New("command not found")
	ErrNotAuthenticated      = errors.New("not authenticated")
	ErrAuthenticationFailure = errors.New("authentication failure")
	ErrCmdParams             = errors.New("invalid command param")
	ErrValue                 = errors.New("value is not an integer or out of range")
	ErrSyntax                = errors.New("syntax error")
	ErrOffset                = errors.New("offset bit is not an natural number")
	ErrBool                  = errors.New("value is not 0 or 1")
)
var errScoreOverflow = errors.New("zset score overflow")

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	RaftDir  string
	RaftBind string

	mu sync.Mutex
	m  map[string]string // The key-value store for the system.

	db *ledis.DB
	ldb *ledis.Ledis

	raft *raft.Raft // The consensus mechanism

	logger *log.Logger

	r redis.Conn
}

// New returns a new Store.
func New() *Store {
	return &Store{
		m:      make(map[string]string),
		logger: log.New(os.Stderr, "[store] ", log.LstdFlags),
		r:		nil,
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomesthe first node, and therefore leader, of the cluster.
func (s *Store) Open(enableSingle bool) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()

	// Check for any existing peers.
	peers, err := readPeersJSON(filepath.Join(s.RaftDir, "peers.json"))
	if err != nil {
		return err
	}

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if enableSingle && len(peers) <= 1 {
		s.logger.Println("enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create peer storage.
	peerStore := raft.NewJSONPeers(s.RaftDir, transport)

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
//	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft-stable.db"))
//	if err != nil {
//		return fmt.Errorf("new bolt store: %s", err)
//	}

	stableStore, err := raftleveldb.NewStore(filepath.Join(s.RaftDir, "raft-level-stable.db"))
	if err != nil {
		return fmt.Errorf("New leveldb store: %s", err)
	}

//	logStore, err := raftleveldb.NewStore(filepath.Join(s.RaftDir, "raft-level-log.db"))
//	if err != nil {
//		return fmt.Errorf("New leveldb store: %s", err)
//	}
	logStore, err := redislogstore.NewStore("localhost:6379")
	if err != nil {
		return fmt.Errorf("New redis log store: %s", err)
	}

	cfg := lediscfg.NewConfigDefault()
	cfg.DBName = "rocksdb"
	dbpath := fmt.Sprintf("fsm-%s.db", cfg.DBName)
	cfg.DataDir = filepath.Join(s.RaftDir, dbpath)
	ldb, err := ledis.Open(cfg)
	if err != nil {
		return fmt.Errorf("ledis open failed: %s", err)
	}
	db, _ := ldb.Select(0)
	s.db = db
	s.ldb = ldb
	s.r = OpenRedis("localhost:6379")

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsmredis)(s), logStore, stableStore, snapshots, peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra


	return nil
}

// Get returns the value for the given key.
func (s *Store) Get(key string) (string, error) {
//	s.mu.Lock()
//	defer s.mu.Unlock()
//	return s.m[key], nil

	if s.r != nil {
		return redis.String(s.r.Do("GET", []byte(key)))
	}

	k := []byte(key)
	value, err := s.db.Get(k)
	if err == nil {
		return string(value), err
	} else {
		s.logger.Printf("db get error %v k %s", err, key)
	}

	return "", err
}

func (s *Store) ApplyRestore(rkv []byte) error {
	if s.raft.State() != raft.Leader {
		leader := s.raft.Leader()
		return fmt.Errorf("not leader " + leader)
	}

	f := s.raft.Apply(rkv, raftTimeout)
	if err, ok := f.(error); ok {
		return err
	}

	if err := f.Error(); err != nil {
		return err
	}

	return  nil
}

// Set sets the value for the given key.
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		leader := s.raft.Leader()
		return fmt.Errorf("not leader " + leader)
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	// FIXME: for data type like sets,
	// if there is an item in the set already,
	// it's better to ignore the sadd operation to
	// prevent creating a new redundant log

	f := s.raft.Apply(b, raftTimeout)
	if err, ok := f.(error); ok {
		return err
	}

	if err = f.Error(); err != nil {
		return err
	}

	return nil
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:  "delete",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	// FIXME: there ia a chance to ignore redundant delete operation

	f := s.raft.Apply(b, raftTimeout)
	if err, ok := f.(error); ok {
		return err
	}

	if err = f.Error(); err != nil {
		return err
	}

	return nil
}

// Join joins a node, located at addr, to this store. The node must be ready to
// respond to Raft communications at that address.
func (s *Store) Join(addr string) error {
	s.logger.Printf("received join request for remote node as %s", addr)

	f := s.raft.AddPeer(addr)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("node at %s joined successfully", addr)
	return nil
}

func (s *Store) Leave(addr string) error {
	s.logger.Printf("received leave request for remote node as %s", addr)

	f := s.raft.RemovePeer(addr)
	if f.Error() != nil {
		return f.Error()
	}

	s.logger.Printf("node at %s left", addr)
	return nil
}

func (s *Store) GetRaft() *raft.Raft {
	return s.raft
}

func encodeRestoreKeyValue(key []byte, ttl int64, data []byte) []byte {
	rkv := make([]byte, 0)
	rkv = append(rkv, 'r')

	key_len_buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(key_len_buf, uint32(len(key)))
	rkv = append(rkv, key_len_buf...)

	rkv = append(rkv, key...)

	ttl_buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(ttl_buf, uint64(ttl))
	rkv = append(rkv, ttl_buf...)

	data_len_buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(data_len_buf, uint32(len(data)))
	rkv = append(rkv, data_len_buf...)

	rkv = append(rkv, data...)

	return rkv
}

func decodeRestoreKeyValue(rkv []byte) ([]byte, int64, []byte, error) {
//	fmt.Printf("rkv: %v\n", rkv)
	if rkv[0] != 'r' {
		return nil, 0, nil, errors.New("Exported r for restore log")
	}
	rkv = rkv[1:]

	key_len := binary.LittleEndian.Uint32(rkv)
	if uint32(len(rkv)) < key_len + 4 {
		return nil, 0, nil, errors.New("Invalid restore log")
	}
	rkv = rkv[4:]
	key := rkv[:key_len]

	rkv = rkv[key_len:]

	ttl := binary.LittleEndian.Uint64(rkv)
	rkv = rkv[8:]

	data_len := binary.LittleEndian.Uint32(rkv)
	if uint32(len(rkv)) < data_len + 4 {
		return nil, 0, nil, errors.New("Invalid restore log")
	}
	rkv = rkv[4:]
	data := rkv[:data_len]

	return key, int64(ttl), data, nil
}

func (s *Store) RestoreAKey(key []byte, ttl int64, data []byte) error {
//	return s.db.Restore(key, ttl, data)
//	rkv := fmt.Sprintf("r,%s,%d,%s", key, ttl, data)
//	fmt.Printf("rkv: %v\n", []byte(rkv))
//	return s.Set(rkv, "1")
	rkv := encodeRestoreKeyValue(key, ttl, data)
	return s.ApplyRestore(rkv)
}

func (s *Store) Select(db int) error {
	// FIXME: select over raft protocol instead of locally
	_, err := s.ldb.Select(db)
	return err
}

func (s *Store) SAdd(key, value string) (int, error) {
	skv := fmt.Sprintf("t,%s,%s", key, value)
	//	FIXME: return the real number
	return 1, s.Set(skv, "1")
}

func (s *Store) SRem(key, value string) (int, error) {
	skv := fmt.Sprintf("t,%s,%s", key, value)
	//	FIXME: return the real number
	return 1, s.Set(skv, "0")
}

func (s *Store) SMembers(key string) ([][]byte, error) {
	return s.db.SMembers([]byte(key))
}

func (s *Store) ZAdd(key []byte, args ...(ledis.ScorePair)) (int64, error) {
	score := args[0].Score
	member := args[0].Member
	zkv := fmt.Sprintf("z,%s,%d,%s", string(key), score, string(member))
	// FIXME: return the real number
	return 1, s.Set(zkv, "1")
}

func (s *Store) ZCard(key []byte) (int64, error) {
	return s.db.ZCard(key)
}

func (s *Store) ZCount(key []byte, min []byte, max []byte) (int64, error) {
	minInt, maxInt, err := zparseScoreRange(min, max)
	if err != nil {
		return 0, ErrValue
	}

	if minInt > maxInt {
		return 0, ErrCmdParams
	}

	return s.db.ZCount(key, minInt, maxInt)
}

func (s *Store) ZRange(key []byte, start int, stop int) ([]ledis.ScorePair, error) {
	return s.db.ZRange(key, start, stop)
}

// key []byte, min []byte, max []byte, offset int, count int
func (s *Store) ZRangeByScore(key []byte, min int64, max int64, offset int, count int) ([]ledis.ScorePair, error) {
	return s.db.ZRangeByScore(key, min, max, offset, count)
}

func (s *Store) ZRangeByScoreGeneric(key []byte, min int64, max int64,
offset int, count int, reverse bool) ([]ledis.ScorePair, error) {
	return s.db.ZRangeByScoreGeneric(key, min, max, offset, count, reverse)
}

func (s *Store) ZScore(key, member []byte) (int64, error) {
	return s.db.ZScore(key, member)
}

type fsmlevel Store

func (f *fsmlevel) Apply(l *raft.Log) interface{} {
	if l.Data[0] == 'r' {
		key, ttl, data, err := decodeRestoreKeyValue(l.Data)
		if err != nil {
			return err
		}

		return f.applyRestore(key, ttl, data)
	}

	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		fmt.Sprintf("failed to unmarshal command: %s", err.Error())
		return nil
	}

//	f.logger.Printf("[DEBUG] fsmlevel apply log %d", l.Index)

	switch c.Op {
	case "set":
		if strings.HasPrefix(c.Key, "t,") {
			skv := strings.Split(c.Key, ",")
			if len(skv) == 3 {
				if c.Value == "0" {
					return f.applySRem(skv[1], skv[2])
				} else {
					return f.applySAdd(skv[1], skv[2])
				}
			}
		} else if strings.HasPrefix(c.Key, "z,") {
			zkv := strings.Split(c.Key, ",")
			if len(zkv) == 4 {
				if c.Value == "0" {
				} else {
					i, err := strconv.ParseInt(zkv[2], 10, 64)
					if err != nil {
						panic(fmt.Sprintf("failed to parse score: %s", err.Error()))
					}
					return f.applyZAdd(zkv[1], i, zkv[3])
				}
			}
		} else {
			return f.applySet(c.Key, c.Value)
		}
	case "delete":
		if strings.HasPrefix(c.Key, "t,") {
			skv := strings.Split(c.Key, ",")
			if len(skv) == 3 {
				return f.applySRem(skv[1], skv[2])
			}
		} else {
			return f.applyDelete(c.Key)
		}
	default:
		panic(fmt.Sprint("unrecognized command op: %s", c.Op))
	}

	return nil
}

func (f *fsmlevel) applyRestore(key []byte, ttl int64, data []byte) interface{} {
//	fmt.Printf("applyRestore k %s t %d d %s\n", string(key), ttl, string(data))

	return f.db.Restore([]byte(key), ttl, []byte(data))
}

func (f *fsmlevel) applyZAdd(key string, score int64, value string) interface{} {
	sp := ledis.ScorePair{Score: score, Member: []byte(value)}
	if num, err := f.db.ZAdd([]byte(key), sp); err != nil {
		return err
	} else {
		num = num
	}

	return nil
}

func (f *fsmlevel) applySAdd(key, value string) interface{} {
	if num, err := f.db.SAdd([]byte(key), []byte(value)); err != nil {
		f.logger.Print("failed to fsmlevel sadd %v", err)
		return err
	} else {
//		f.logger.Print("sadd ", num)
		num = num
	}
	return nil
}

func (f *fsmlevel) applySRem(key, value string) interface{} {
	if num, err := f.db.SRem([]byte(key), []byte(value)); err != nil {
		f.logger.Print("failed to fsmlevel srem %v", err)
		return err
	} else {
//		f.logger.Print("srem ", num)
		num = num
	}

	return nil
}

func (f *fsmlevel) applySet(key, value string) interface{} {
//	f.logger.Printf("fsmlevel set k %s v %s", key, value)
	if err := f.db.Set([]byte(key), []byte(value)); err != nil {
		f.logger.Printf("failed to fsmlevel set %v", err)
		return err
	}
	return nil
}

func (f *fsmlevel) applyDelete(key string) interface{} {
//	f.logger.Printf("fsmlevel del k %s", key)
	_, err := f.db.Del([]byte(key))
	if err != nil {
		f.logger.Printf("failed to fsmlevel del %v", err)
		return err
	}
	return nil
}

func (f *fsmlevel) Snapshot() (raft.FSMSnapshot, error) {
//	f.ldb.DumpFile(filepath.Join(f.RaftDir, "fsmlevel-dump"))
	// TODO
	// 1. create a snapshot in leveldb/ledisdb
	snap, err := f.ldb.Snapshot()
	// 2. return the snapshot ID
	return &fsmlevelSnapshot{snap: snap, ldb: f.ldb}, err
}

func (f *fsmlevel) Restore(rc io.ReadCloser) error {
	f.logger.Printf("Start restore")
	_, err := f.ldb.LoadDump(rc)
	return err
}

type fsmlevelSnapshot struct {
	store map[string]string
	snap *ledisstore.Snapshot
	ldb *ledis.Ledis
}

func (f *fsmlevelSnapshot) Persist(sink raft.SnapshotSink) error {
	err := f.ldb.Dump(sink)

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmlevelSnapshot) Release() {
	f.snap.Close()
}

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}

	o := make(map[string]string)
	if err := json.Unmarshal(b, &o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}

func readPeersJSON(path string) ([]string, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if len(b) == 0 {
		return nil, nil
	}

	var peers []string
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&peers); err != nil {
		return nil, err
	}

	return peers, nil
}

func zparseScoreRange(minBuf []byte, maxBuf []byte) (min int64, max int64, err error) {
	if strings.ToLower(hack.String(minBuf)) == "-inf" {
		min = math.MinInt64
	} else {

		if len(minBuf) == 0 {
			err = ErrCmdParams
			return
		}

		var lopen bool = false
		if minBuf[0] == '(' {
			lopen = true
			minBuf = minBuf[1:]
		}

		min, err = ledis.StrInt64(minBuf, nil)
		if err != nil {
			err = ErrValue
			return
		}

		if min <= ledis.MinScore || min >= ledis.MaxScore {
			err = errScoreOverflow
			return
		}

		if lopen {
			min++
		}
	}

	if strings.ToLower(hack.String(maxBuf)) == "+inf" {
		max = math.MaxInt64
	} else {
		var ropen = false

		if len(maxBuf) == 0 {
			err = ErrCmdParams
			return
		}
		if maxBuf[0] == '(' {
			ropen = true
			maxBuf = maxBuf[1:]
		}

		if maxBuf[0] == '(' {
			ropen = true
			maxBuf = maxBuf[1:]
		}

		max, err = ledis.StrInt64(maxBuf, nil)
		if err != nil {
			err = ErrValue
			return
		}

		if max <= ledis.MinScore || max >= ledis.MaxScore {
			err = errScoreOverflow
			return
		}

		if ropen {
			max--
		}
	}

	return
}
