package redisd

import (
	"fmt"
	"log"

	redis "github.com/dotcloud/go-redis-server"
)

// Store is the interface Raft-backed key-value stores must implement.
type Store interface {
	// Get returns the value for the given key.
	Get(key string) (string, error)

	// Set sets the value for the given key, via distributed consensus.
	Set(key, value string) error

	// Delete removes the given key, via distributed consensus.
	Delete(key string) error

	// Join joins the node, reachable at addr, to the cluster.
	Join(addr string) error
}

type Service struct {
	host string
	port int
	store Store
}

func New(host string, port int, store Store) *Service {
	return &Service{
		host:	host,
		port:	port,
		store:	store,
	}
}

type MyHandler struct {
	values map[string] string
	store Store
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

func (h *MyHandler) Ping() (string, error) {
	return "pong", nil
}

func (s *Service) Start() {
	defer func() {
		if msg := recover(); msg != nil {
			fmt.Printf("Panic: %v\n", msg)
		}
	}()

	log.Println("redis listen", s.host, s.port)

	myhandler := &MyHandler{values: make(map[string] string), store: s.store}
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