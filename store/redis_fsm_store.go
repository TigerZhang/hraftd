package store

import (
//	"github.com/garyburd/redigo/redis"
//	"log"
	"github.com/TigerZhang/raft"
	"io"
	"fmt"
	"encoding/json"
	"github.com/Sirupsen/logrus"
//	"time"
)

var logr = logrus.New()

func init() {
	//	logrus.Formatter = new(logrus.JSONFormatter)
	logr.Formatter = new(logrus.TextFormatter) // default
	logr.Level = logrus.WarnLevel
}

type fsmredisSnapshot struct {
}

func (f *fsmredisSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (f *fsmredisSnapshot) Release() {

}

type fsmredis Store

//func OpenRedis(target string) redis.Conn {
////	c, err := redis.Dial("tcp", target)
//	c, err := redis.DialTimeout("tcp", target, 3 * time.Second, 5 * time.Second, 5 * time.Second)
//	if err != nil {
//		log.Panicf("cannot connect to '%s'", target)
//	}
//
//	return c
//}

func (f *fsmredis) Apply(l *raft.Log) interface{} {
	logr.Debug("fsmredis apply %v", l)
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

func (f *fsmredis) applySet(key, value string) interface{} {
	c := f.rpool.Get()
	defer c.Close()

//	reply, err := f.r.Do("SET", key, value)
	reply, err := c.Do("SET", key, value)
	if err != nil {
		logr.Errorf("applySet failed. reply %v err %v", reply, err)
	}
	return err
}

func (f *fsmredis) applyDelete(key string) interface{} {
	c := f.rpool.Get()
	defer c.Close()

//	reply, err := f.r.Do("DEL", key)
	reply, err := c.Do("DEL", key)
	if err != nil {
		logr.Errorf("applyDelete failed. reply %v err %v", reply, err)
	}
	return err
}

func (f *fsmredis) Snapshot() (raft.FSMSnapshot, error) {
	// find latest LastIndex in AOF file of log store,
	// check
	return &fsmredisSnapshot{}, nil
}

func (f *fsmredis) Restore(rc io.ReadCloser) error {
	return nil
}