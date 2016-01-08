package redisd

import (
	"fmt"
	"log"
	"strconv"

	"github.com/TigerZhang/raft"
	redis "github.com/TigerZhang/go-redis-server"
	"github.com/TigerZhang/ledisdb/ledis"
)

// Store is the interface Raft-backed key-value stores must implement.
type Store interface {
	// Get returns the value for the given key.
	Get(key string) (string, error)

	// Set sets the value for the given key, via distributed consensus.
	Set(key, value string) error

	// Delete removes the given key, via distributed consensus.
	Delete(key string) error

	SAdd(key, value string) (int, error)
	SRem(key, value string) (int, error)
	SMembers(key string) ([][]byte, error)

	ZAdd(key []byte, args ...(ledis.ScorePair)) (int64, error)
	ZCard(key []byte) (int64, error)
	ZRange(key []byte, start int, stop int) ([]ledis.ScorePair, error)
	ZRangeByScore(key []byte, min int64, max int64, offset int, count int) ([]ledis.ScorePair, error)

	// Join joins the node, reachable at addr, to the cluster.
	Join(addr string) error
}

type Service struct {
	host string
	port int
	store Store
	raft *raft.Raft
}

func New(host string, port int, store Store, raft *raft.Raft) *Service {
	return &Service{
		host:	host,
		port:	port,
		store:	store,
		raft:	raft,
	}
}

type MyHandler struct {
	values map[string] string
	store Store
	raft *raft.Raft
}

func (h *MyHandler) Get(key string) (string, error) {
//	v := h.values[key]
	v, err := h.store.Get(key)
	return v, err
}

func (h *MyHandler) Set(key string, value string) error {
//	h.values[key] = value
	err := h.store.Set(key, value)
	return err
}

func (h *MyHandler) Del(key string) error {
	return h.store.Delete(key)
}

func (h *MyHandler) Ping() ([]byte, error) {
	return []byte("PONG"), nil
}

func (h *MyHandler) Leader() (string, error) {
	return h.raft.Leader(), nil
}

func (h *MyHandler) SMembers(key string) ([][]byte, error) {
	return h.store.SMembers(key)
}

func (h *MyHandler) SAdd(key, value string) (int, error) {
	return h.store.SAdd(key, value)
}

func (h *MyHandler) SRem(key, value string) (int, error) {
	return h.store.SRem(key, value)
}

func (h *MyHandler) ZAdd(key, score, value string) (int, error) {
	i, err := strconv.ParseInt(score, 10, 64)
	if err != nil {
		return 0, err
	}
	sp := ledis.ScorePair{Score: i, Member: []byte(value)}
	num, err := h.store.ZAdd([]byte(key), sp)
	return int(num), err
}

func (h *MyHandler) ZCard(key string) (int, error) {
	num, err := h.store.ZCard([]byte(key))
	return int(num), err
}

func (h *MyHandler) ZRange(key, start, stop string) ([][]byte, error){
	starti, err := strconv.ParseInt(start, 10, 32)
	if err != nil {
		return nil, err
	}
	stopi, err := strconv.ParseInt(stop, 10, 32)
	if err != nil {
		return nil, err
	}

	result := make([][]byte, 0, 16)
	sp, err := h.store.ZRange([]byte(key), int(starti), int(stopi))
//	fmt.Printf("zrange result: %v", sp)
	for _, v := range sp {
		result = append(result, v.Member)
	}
	return result, err
}

func (s *Service) Start(raft *raft.Raft) {
	defer func() {
		if msg := recover(); msg != nil {
			fmt.Printf("Panic: %v\n", msg)
		}
	}()

	log.Println("redis listen", s.host, s.port)

	myhandler := &MyHandler{values: make(map[string] string), store: s.store, raft: raft}
//	myhandler := redis.NewDefaultHandler()
	srv, err := redis.NewServer(redis.DefaultConfig().Host(s.host).Port(s.port).Handler(myhandler))
	if err != nil {
		panic(err)
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			panic(err)
		}
	}()
}