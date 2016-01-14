package store

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
	"reflect"
	"fmt"
)

func Test_StoreOpen(t *testing.T) {
	s := New()
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)

	s.RaftBind = "127.0.0.1:0"
	s.RaftDir = tmpDir
	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(false); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
}

func Test_StoreOpenSingleNode(t *testing.T) {
	s := New()
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)

	s.RaftBind = "127.0.0.1:0"
	s.RaftDir = tmpDir
	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	if err := s.Set("foo", "bar"); err != nil {
		t.Fatalf("failed to set key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	value, err := s.Get("foo")
	if err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}
	if value != "bar" {
		t.Fatalf("key has wrong value: %s", value)
	}

	if err := s.Delete("foo"); err != nil {
		t.Fatalf("failed to delete key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	value, err = s.Get("foo")
	if err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}
	if value != "" {
		t.Fatalf("key has wrong value: %s", value)
	}

}

func Test_RestoreEncodeDecode(t *testing.T) {
	key := []byte{'1','2','3',0,'4'}
	ttl := int64(123)
	data := []byte{'2','3','4',0,'5'}
	rkv := encodeRestoreKeyValue(key, ttl, data)

	key2, ttl2, data2, err := decodeRestoreKeyValue(rkv)
	if err != nil {
		t.Fatalf("decode failed: %s", err.Error())
	}

	if !reflect.DeepEqual(key2, key) {
		t.Fatal("key error")
	}

	if ttl2 != ttl {
		t.Fatal("ttl error")
	}

	if !reflect.DeepEqual(data2, data) {
		fmt.Printf("data: %v data2: %v\n", data, data2)
		t.Fatal("data error")
	}
}
