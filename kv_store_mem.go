package turing

import (
	"time"
	"sync"
)

type KVStoreMemory struct {
	kv map[string]interface{}
	ex map[string]time.Time
	closeChan chan struct{}
	rw sync.RWMutex
}

type stringValue struct {
	value interface{}
}

type hashMapValue struct {
	kvMap map[string]interface{}
}

type genericValue struct {
	value interface{}
}

func (kvs *KVStoreMemory) isString(key string) bool {
	_, ok := kvs.kv[key].(stringValue)
	return ok
}

func (kvs *KVStoreMemory) isHashMap(key string) bool {
	_, ok := kvs.kv[key].(hashMapValue)
	return ok
}

func (kvs *KVStoreMemory) isGeneric(key string) bool {
	_, ok := kvs.kv[key].(genericValue)
	return ok
}

func (kvs *KVStoreMemory) hashKeyExists(key string, field string) bool {
	_, ok := kvs.kv[key].(hashMapValue).kvMap[key]
	return ok
}

func (kvs *KVStoreMemory) convertToStringMap(m map[string]interface{}) map[string]string {
	newMap := make(map[string]string)
	for k, v := range m {
		s, ok := v.(string)
		if ok {
			newMap[k] = s
		}
	}
	return newMap
}

func (kvs *KVStoreMemory) exists(key string) bool {
	_, ok := kvs.kv[key]
	t, ok2 := kvs.ex[key]
	return ok && (!ok2 || time.Now().Before(t))
}

func (kvs *KVStoreMemory) deleteExpired() {
	kvs.rw.Lock()
	defer kvs.rw.Unlock()
	for k, v := range kvs.ex {
		if time.Now().After(v) {
			_, ok := kvs.kv[k]
			if ok {
				delete(kvs.kv, k)
			}
			delete(kvs.ex, k)
		}
	}
}

func (kvs *KVStoreMemory) run() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <- kvs.closeChan:
			return
		case <- ticker.C:
			kvs.deleteExpired()
		}
	}
}

func (kvs *KVStoreMemory) Set(key string, value interface{}) error {
	kvs.rw.Lock()
	defer kvs.rw.Unlock()
	if kvs.exists(key) && !kvs.isString(key) {
		return WrongTypeError
	}
	kvs.kv[key] = stringValue{
		value: value,
	}
	return nil
}

func (kvs *KVStoreMemory) Get(key string) (string, error) {
	kvs.rw.RLock()
	defer kvs.rw.RUnlock()
	if !kvs.exists(key) {
		return "", KeyNotExistsError
	} else if !kvs.isString(key) {
		return "", WrongTypeError
	}
	return kvs.kv[key].(stringValue).value.(string), nil
}

func (kvs *KVStoreMemory) Exists(key string) bool {
	kvs.rw.RLock()
	defer kvs.rw.RUnlock()
	return kvs.exists(key)
}

func (kvs *KVStoreMemory) Delete(keys ...string) int {
	kvs.rw.Lock()
	defer kvs.rw.Unlock()
	c := 0
	for _, key := range keys {
		if kvs.exists(key) {
			c++
			_, ok := kvs.ex[key]
			if ok {
				delete(kvs.ex, key)
			}
			delete(kvs.kv, key)
		}
	}
	return c
}

func (kvs *KVStoreMemory) HSet(key string, field string, value interface{}) error {
	kvs.rw.Lock()
	defer kvs.rw.Unlock()
	if kvs.exists(key) && !kvs.isHashMap(key) {
		return WrongTypeError
	} else if !kvs.exists(key) {
		kvs.kv[key] = hashMapValue{
			kvMap: make(map[string]interface{}),
		}
	}
	kvs.kv[key].(hashMapValue).kvMap[field] = value
	return nil
}

func (kvs *KVStoreMemory) HSetMany(key string, kv map[string]interface{}) error {
	kvs.rw.Lock()
	defer kvs.rw.Unlock()
	if kvs.exists(key) && !kvs.isHashMap(key) {
		return WrongTypeError
	} else if !kvs.exists(key) {
		kvs.kv[key] = hashMapValue{
			kvMap: make(map[string]interface{}),
		}
	}
	for k, v := range kv {
		kvs.kv[key].(hashMapValue).kvMap[k] = v
	}
	return nil
}

func (kvs *KVStoreMemory) HGet(key string, field string) (string, error) {
	kvs.rw.RLock()
	defer kvs.rw.RUnlock()
	if !kvs.exists(key) {
		return "", KeyNotExistsError
	} else if !kvs.isHashMap(key) {
		return "", WrongTypeError
	} else if !kvs.hashKeyExists(key, field) {
		return "", KeyNotExistsError
	} else {
		return kvs.kv[key].(hashMapValue).kvMap[field].(string), nil
	}
}

func (kvs *KVStoreMemory) HGetAll(key string) (map[string]string, error) {
	kvs.rw.RLock()
	defer kvs.rw.RUnlock()
	if !kvs.exists(key) {
		return make(map[string]string), nil
	} else if !kvs.isHashMap(key) {
		return nil, WrongTypeError
	} else {
		return kvs.convertToStringMap(kvs.kv[key].(hashMapValue).kvMap), nil
	}
}

func (kvs *KVStoreMemory) HDelete(key string, fields []string) (int, error) {
	kvs.rw.Lock()
	defer kvs.rw.Unlock()
	if !kvs.exists(key) {
		return 0, nil
	} else if !kvs.isHashMap(key) {
		return 0, WrongTypeError
	} else {
		c := 0
		m := kvs.kv[key].(hashMapValue).kvMap
		for _, field := range fields {
			_, ok := m[field]
			if ok {
				c++
				delete(m, field)
			}
		}
		if len(m) == 0 {
			delete(kvs.kv, key)
		}
		return c, nil
	}
}

func (kvs *KVStoreMemory) Expire(key string, expiry time.Duration) error {
	kvs.rw.Lock()
	defer kvs.rw.Unlock()
	if !kvs.exists(key) {
		return KeyNotExistsError
	}
	kvs.ex[key] = time.Now().Add(expiry)
	return nil
}

func (kvs *KVStoreMemory) SetGeneric(key string, value interface{}) {
	kvs.rw.Lock()
	defer kvs.rw.Unlock()
	kvs.kv[key] = genericValue{
		value: value,
	}
}

func (kvs *KVStoreMemory) GetGeneric(key string) (interface{}, error) {
	kvs.rw.RLock()
	defer kvs.rw.RUnlock()
	if !kvs.exists(key) {
		return nil, KeyNotExistsError
	} else if !kvs.isGeneric(key) {
		return nil, WrongTypeError
	} else {
		return kvs.kv[key].(genericValue).value, nil
	}
}

func (kvs *KVStoreMemory) Close() {
	close(kvs.closeChan)
}

func NewKVStoreMemory() *KVStoreMemory {
	kvs := &KVStoreMemory{
		kv: make(map[string]interface{}),
		ex: make(map[string]time.Time),
		closeChan: make(chan struct{}),
	}
	go kvs.run()
	return kvs
}