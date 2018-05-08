package redis

import (
	"time"
	"sync"
)

type MockDB struct {
	kv map[string]*generalValue
	rw sync.RWMutex
}

type generalValue struct {
	val interface{}
	ex time.Time
	isEx bool
}

func (mdb *MockDB) Set(key string, value interface{}) {
	mdb.rw.Lock()
	defer mdb.rw.Unlock()
	mdb.kv[key] = &generalValue{
		val: value.(string),
		ex: time.Now(),
		isEx: false,
	}
}

func (mdb *MockDB) run() {
	for {
		mdb.rw.Lock()
		defer mdb.rw.Unlock()

		for k, v := range mdb.kv {
			if v.isEx && time.Now().After(v.ex) {
				delete(mdb.kv, k)
			}
		}

		time.Sleep(time.Millisecond)
	}
}

func NewMockDB() *MockDB {
	mdb := &MockDB{
		kv: make(map[string]*generalValue),
	}

	go mdb.run()
	return mdb
}