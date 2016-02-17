package redislogstore

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"github.com/TigerZhang/raft"
	"bytes"

	"github.com/Sirupsen/logrus"

	"github.com/hashicorp/go-msgpack/codec"
	"encoding/binary"
//	"fmt"
	"strconv"
	"time"
)

var logr = logrus.New()

func init() {
//	logrus.Formatter = new(logrus.JSONFormatter)
	logr.Formatter = new(logrus.TextFormatter) // default
	logr.Level = logrus.WarnLevel
}

type Store struct {
	db redis.Conn
	rpool *redis.Pool

	firstIndex uint64
	lastIndex uint64

	target string
	password string
}

func NewStore(target string) (*Store, error) {
	c, err := ConnectRedis(target, "")
	pool := NewRedisPool(target, "")

	firstIndex, err := redis.Uint64(c.Do("GET", "FirstIndex"))
	if err == nil {
		if err != nil {
			return nil, err
		}
	}

	lastIndex, err := redis.Uint64(c.Do("GET", "LastIndex"))
	if err == nil {
		if err != nil {
			return nil, err
		}
	}

	return &Store{
		db: c,
		rpool: pool,
		firstIndex: firstIndex,
		lastIndex: lastIndex,
		target: target,
		password: "",
	}, nil
}

func ConnectRedis(target, password string) (redis.Conn, error) {
//	c, err := redis.Dial("tcp", target)
	c, err := redis.DialTimeout("tcp", target, 3 * time.Second, 5 * time.Second, 5 * time.Second)
	if err != nil {
		log.Panicf("cannot connect to '%s'", target)
	}

	if err := authPassword(c, password); err != nil {
		return nil, err
	}

	return c, err
}

func NewRedisPool(target, password string) *redis.Pool {
	RedisClient := &redis.Pool{
		// 从配置文件获取maxidle以及maxactive，取不到则用后面的默认值
		MaxIdle:     5,
		MaxActive:   30,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redis.Conn, error) {
//			c, err := redis.Dial("tcp", target)
			c, err := redis.DialTimeout("tcp", target, 3 * time.Second, 5 * time.Second, 5 * time.Second)
			if err != nil {
				return nil, err
			}
			// 选择db
			c.Do("SELECT", 0)
			return c, nil
		},
	}

	return RedisClient
}

func (s *Store) reconnectRedis() {
	logr.Debug("reconnectRedis")

	s.db.Close()

	c, err := ConnectRedis(s.target, s.password)
	if err != nil {
		log.Panicf("reconnect redis failed %v", err)
	}
	s.db = c
}

// for test only
func (s *Store) Flushall() {
	s.db.Do("FLUSHALL")
}

func authPassword(c redis.Conn, passwd string) error {
	if passwd == "" {
		return nil
	}
	if _, err := c.Do("AUTH", passwd); err != nil {
		c.Close()
		return err
	}

	return nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) FirstIndex() (uint64, error) {
	logr.Debug("FirstIndex")
	c := s.rpool.Get()
	defer c.Close()

	if firstIndex, err := redis.Uint64(c.Do("GET", "FirstIndex")); err != nil {
		return 0, nil
	} else {
		return firstIndex, err
	}
}

func (s *Store) LastIndex() (uint64, error) {
	logr.Debug("LastIndex")
	c := s.rpool.Get()
	defer c.Close()

	if lastIndex, err := redis.Uint64(c.Do("GET", "LastIndex")); err != nil {
		return 0, nil
	} else {
		return lastIndex, err
	}
}

func (s *Store) GetLog(index uint64, log *raft.Log) error {
//	key := uint64ToBytes(index)
	c := s.rpool.Get()
	defer c.Close()

	v, err := redis.Bytes(c.Do("GET", index))
//	fmt.Printf("v = %v err = %v\n", v, err)
	logr.Debugf("log: %v", v)
	if err != nil {
		return raft.ErrLogNotFound
	}

	return decodeMsgPack(v, log)
}

func (s *Store) StoreLogs(logs []*raft.Log) error {
	// MSET
	for _, log := range logs {
		s.StoreLog(log)
	}
	return nil
}

func (s *Store) StoreLog(log *raft.Log) error {
	//	key := uint64ToBytes(log.Index)
	val, err := encodeMsgPack(log)
	if err != nil {
		return err
	}

	// TODO: Redis MSET
	//		if _, err := s.db.Do("SET", log.Index, val); err != nil {
	//			return err
	//		}
	cmd := make([]interface{}, 0)
	cmd = append(cmd, strconv.FormatUint(log.Index, 10), val.String())
	cmdIndex := s.updateIndex(log.Index, true)
	cmd = append(cmd, cmdIndex...)

	c := s.rpool.Get()
	defer c.Close()

	if _, err := c.Do("MSET", cmd...); err != nil {
		s.reconnectRedis()

		_, err = c.Do("MSET", cmd...)
		if err != nil {
			logr.Panicf("StoreLog failed. c %v e %v", cmd, err)
		}
		return err
	}
	return nil
}

func (s *Store) DeleteRange(min, max uint64) error {
	c := s.rpool.Get()
	defer c.Close()

	// MDEL
	for i := min; i<=max; i++ {
		cmd := s.updateIndex(i, false)

		if len(cmd) > 0 {

			if _, err := c.Do("MSET", cmd...); err != nil {
				//		key := uint64ToBytes(i)
				logr.Errorf("Update index failed. %v %v", cmd, err)
				s.reconnectRedis()
				continue
			}
		}

		if _, err := c.Do("DEL", i); err != nil {
			// return err
			logr.Errorf("Del log failed. i: %d %v", i, err)
			s.reconnectRedis()
		}
	}
	return nil
}

func (s *Store) Set(key []byte, val []byte) error {
	// SET
	c := s.rpool.Get()
	defer c.Close()

	logr.Debugf("set k %s v %s", string(key), string(val))
	if _, err := c.Do("SET", key, val); err != nil {
		return err
	}

	return nil
}

func (s *Store) Get(key []byte) ([]byte, error) {
	// GET
	c := s.rpool.Get()
	defer c.Close()

	logr.Debugf("get k %s", string(key))
	v, err := redis.Bytes(c.Do("GET", key));
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (s *Store) updateIndex(index uint64, store bool) []interface{} {
	first_origin := s.firstIndex
	last_origin := s.lastIndex

	// first update
	if s.firstIndex == 0 {
		s.firstIndex = index
	} else {
		if store {
			// insert log less than firstInddex ???
			if index < s.firstIndex {
				s.firstIndex = index
			}
		} else {
			// delete log less than firstIndex ???
			if index<=s.firstIndex {
				s.firstIndex = index+1
			}
		}
	}

	if s.lastIndex == 0 {
		s.lastIndex = index
	} else {
		if store {
			// insert log after lastIndex
			if index > s.lastIndex {
				s.lastIndex = index
			}
		} else {
			// delete log less than lastIndex
			if index<=s.lastIndex {
				s.lastIndex = index-1
			}
		}
	}

	cmd := make([]interface{}, 0)
	if first_origin != s.firstIndex {
//		s.db.Do("SET", "FirstIndex", s.firstIndex)
//		fmt.Printf("Set FirstIndex to: %d\n", s.firstIndex)
//		fi := strconv.FormatUint(s.firstIndex, 10)
		cmd = append(cmd, "FirstIndex", s.firstIndex)
	}

	if last_origin != s.lastIndex {
//		s.db.Do("SET", "LastIndex", s.lastIndex)
//		fmt.Printf("Set LastIndex to: %d\n", s.lastIndex)
//		li := strconv.FormatUint(s.lastIndex, 10)
		cmd = append(cmd, "LastIndex", s.lastIndex)
	}

	return cmd
}

// Decode reverses the encode operation on a byte slice input
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
