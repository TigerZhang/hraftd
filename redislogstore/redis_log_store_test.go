package redislogstore

import (
	"testing"

	"github.com/TigerZhang/raft"
	"time"
)

func TestLogs(t *testing.T) {
	l, err := NewStore("localhost:6379")
	defer l.Close()

	l.Flushall()

	time.Sleep(time.Second * 5)

	// Should be no first index
	idx, err := l.FirstIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 0 {
		t.Fatalf("bad idx: %d", idx)
	}

	// Should be no last index
	idx, err = l.LastIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 0 {
		t.Fatalf("bad idx: %d", idx)
	}

	// Try a filed fetch
	var out raft.Log
	if err := l.GetLog(10, &out); err.Error() != "log not found" {
		t.Fatalf("err: %v ", err)
	}

	// Write out a log
	log := raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  []byte("first"),
	}
	for i := 1; i <= 10; i++ {
		log.Index = uint64(i)
		log.Term = uint64(i)
		if err := l.StoreLog(&log); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// Attempt to write multiple logs
	var logs []*raft.Log
	for i := 11; i <= 20; i++ {
		nl := &raft.Log{
			Index: uint64(i),
			Term:  uint64(i),
			Type:  raft.LogCommand,
			Data:  []byte("first"),
		}
		logs = append(logs, nl)
	}
	if err := l.StoreLogs(logs); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Try to fetch
	if err := l.GetLog(10, &out); err != nil {
		t.Fatalf("err: %v ", err)
	}

	// Try to fetch
	if err := l.GetLog(20, &out); err != nil {
		t.Fatalf("err: %v ", err)
	}

	// Check the lowest index
	idx, err = l.FirstIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 1 {
		t.Fatalf("bad idx: %d", idx)
	}

	// Check the highest index
	idx, err = l.LastIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 20 {
		t.Fatalf("bad idx: %d", idx)
	}

	// Delete a suffix
	if err := l.DeleteRange(5, 20); err != nil {
		t.Fatalf("err: %v ", err)
	}

	// Verify they are all deleted
	for i := 5; i <= 20; i++ {
		if err := l.GetLog(uint64(i), &out); err != raft.ErrLogNotFound {
			t.Fatalf("err: %v ", err)
		}
	}

	// Index should be one
	idx, err = l.FirstIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 1 {
		t.Fatalf("bad idx: %d", idx)
	}
	idx, err = l.LastIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 4 {
		t.Fatalf("bad idx: %d", idx)
	}

	// Should not be able to fetch
	if err := l.GetLog(5, &out); err.Error() != "log not found" {
		t.Fatalf("err: %v ", err)
	}
}

