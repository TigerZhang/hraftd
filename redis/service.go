package redisd

import (
	"fmt"
	"log"
	"strconv"
	"errors"

	"github.com/TigerZhang/raft"
	redis "github.com/TigerZhang/go-redis-server"
	"github.com/TigerZhang/ledisdb/ledis"
	"strings"
	"github.com/siddontang/go/hack"
"math"
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
	ZCount(key []byte, min []byte, max []byte) (int64, error)
	ZRange(key []byte, start int, stop int) ([]ledis.ScorePair, error)
	ZRangeByScore(key []byte, min int64, max int64, offset int, count int) ([]ledis.ScorePair, error)
	ZScore(key, member []byte) (int64, error)
	ZRangeByScoreGeneric(key []byte, min int64, max int64, offset int, count int, reverse bool) ([]ledis.ScorePair, error)

	// Join joins the node, reachable at addr, to the cluster.
	Join(addr string) error
}

type Service struct {
	host string
	port int
	store Store
	raft *raft.Raft
}

type RangeGeneric struct {
	sp []ledis.ScorePair
	score bool
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

func (h *MyHandler) ZCount(key, min, max string) (int, error) {
	num, err := h.store.ZCount([]byte(key), []byte(min), []byte(max))
	return int(num), err
}

func (h *MyHandler) ZRange(key, start, stop string, withScores...string) ([][]byte, error){
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

	var withscores bool = false
	if len(withScores) == 1 {
		if strings.ToLower(withScores[0]) == "withscores" {
			withscores = true
		}
	}
	for _, v := range sp {
		result = append(result, v.Member)
		if withscores {
			result = append(result, []byte(fmt.Sprintf("%d", v.Score)))
		}
	}
	return result, err
}

func (h *MyHandler) ZRangeByScore(args...string) ([][]byte, error) {
	argsA := make([][]byte, 0, 16)
	for _, v := range args {
		argsA = append(argsA, []byte(v))
	}

	rangeGeneric, err := h.zrangeByScoreGeneric(argsA)

	if err != nil {
		return nil, err
	}

	result := make([][]byte, 0, 16)
	for _, v := range rangeGeneric.sp {
		result = append(result, v.Member)
		if rangeGeneric.score {
			result = append(result, []byte(fmt.Sprintf("%d", v.Score)))
		}
	}

	return result, nil
}

func (h *MyHandler) ZScore(key, member string) (int, error) {
	num, err := h.store.ZScore([]byte(key), []byte(member))
	return int(num), err
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

func (h *MyHandler) zrangeByScoreGeneric(args [][]byte) (*RangeGeneric, error) {
	rangeGeneric := &RangeGeneric{sp: nil, score: false}

	if len(args) < 3 {
		return rangeGeneric, ErrCmdParams
	}

	key := args[0]

	var minScore, maxScore []byte

	reverse := false
	if !reverse {
		minScore, maxScore = args[1], args[2]
	} else {
		minScore, maxScore = args[2], args[1]
	}

	min, max, err := zparseScoreRange(minScore, maxScore)

	if err != nil {
		return rangeGeneric, err
	}

	args = args[3:]

	if len(args) > 0 {
		if strings.ToLower(hack.String(args[0])) == "withscores" {
			rangeGeneric.score = true
			args = args[1:]
		}
	}

	var offset int = 0
	var count int = -1

	if len(args) > 0 {
		if len(args) != 3 {
			return rangeGeneric, ErrCmdParams
		}

		if strings.ToLower(hack.String(args[0])) != "limit" {
			return rangeGeneric, ErrSyntax
		}

		if offset, err = strconv.Atoi(hack.String(args[1])); err != nil {
			return rangeGeneric, ErrValue
		}

		if count, err = strconv.Atoi(hack.String(args[2])); err != nil {
			return rangeGeneric, ErrValue
		}
	}

	if offset < 0 {
		//for ledis, if offset < 0, a empty will return
		//so here we directly return a empty array
		return rangeGeneric, ErrCmdParams
	}

	if datas, err := h.store.ZRangeByScoreGeneric(key, min, max, offset, count, reverse); err != nil {
		return rangeGeneric, err
	} else {
		rangeGeneric.sp = datas
		return rangeGeneric, nil
	}
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

