package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/otoolep/hraftd/http"
	"github.com/otoolep/hraftd/store"
	redisd "github.com/otoolep/hraftd/redis"
)

const (
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":12000"

	DefaultRedisBindHost = "127.0.0.1"
	DefaultRedisPort = 6389
)

// Parameters
var httpAddr string
var raftAddr string
var joinAddr string

var redisBindHost string
var redisPort int

func init() {
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&redisBindHost, "redisBind", DefaultRedisBindHost, "Set the redis bind host")
	flag.IntVar(&redisPort, "redisPort", DefaultRedisPort, "Set the redis bind port")
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}

	// Ensure Raft storage exists.
	raftDir := flag.Arg(0)
	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(raftDir, 0700)

	s := store.New()
	s.RaftDir = raftDir
	s.RaftBind = raftAddr
	if err := s.Open(joinAddr == ""); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	h := httpd.New(httpAddr, s)
	if err := h.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}

	// If join was specified, make the join request.
	if joinAddr != "" {
		if err := join(joinAddr, raftAddr); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
	}

	r := redisd.New(redisBindHost, redisPort, s)
	r.Start()

	log.Println("hraft started successfully")

	select {}
}

func join(joinAddr, raftAddr string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
