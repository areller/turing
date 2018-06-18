package redis

import (
	"sync"
	"time"
	"github.com/areller/turing"
	"container/list"
	"strconv"
)

type listValue struct {
	lst *list.List
	rw *sync.RWMutex
}

type setValue struct {
	set map[string]bool
	rw *sync.RWMutex
}

type dbMockInternal struct {
	subs map[string][]func(key string, value interface{})
	rw sync.RWMutex
}

type DBMock struct {
	*turing.KVStoreMemory
	*dbMockInternal
}

func (dbm *DBMock) isList(key string) bool {
	g, err := dbm.GetGeneric(key)
	if err != nil {
		return false
	}
	_, ok := g.(listValue)
	return ok
}

func (dbm *DBMock) getList(key string) listValue {
	g, _ := dbm.GetGeneric(key)
	return g.(listValue)
}

func (dbm *DBMock) SetWithExpiry(key string, value interface{}, expiry time.Duration) error {
	err := dbm.Set(key, value)
	if err != nil {
		return err
	}
	dbm.Expire(key, expiry)
	return nil
}

func (dbm *DBMock) LPush(key string, values ...interface{}) error {
	g, err := dbm.GetGeneric(key)
	if err == turing.WrongTypeError {
		return err
	} else if err == nil && !dbm.isList(key) {
		return turing.WrongTypeError
	} else if err == turing.KeyNotExistsError {
		g = listValue{
			lst: list.New(),
			rw: new(sync.RWMutex),
		}
		dbm.SetGeneric(key, g)
	}

	g.(listValue).rw.Lock()
	defer g.(listValue).rw.Unlock()
	for val := range values {
		g.(listValue).lst.PushFront(val)
	}
	return nil
}

func (dbm *DBMock) RPush(key string, values ...interface{}) error {
	g, err := dbm.GetGeneric(key)
	if err == turing.WrongTypeError {
		return err
	} else if err == nil && !dbm.isList(key) {
		return turing.WrongTypeError
	} else if err == turing.KeyNotExistsError {
		g = listValue{
			lst: list.New(),
			rw: new(sync.RWMutex),
		}
		dbm.SetGeneric(key, g)
	}

	g.(listValue).rw.Lock()
	defer g.(listValue).rw.Unlock()
	for val := range values {
		g.(listValue).lst.PushBack(val)
	}
	return nil
}

func (dbm *DBMock) LTrim(key string, start int64, end int64) error {
	if !dbm.Exists(key) {
		return nil
	} else if !dbm.isList(key) {
		return turing.WrongTypeError
	} else {
		glst := dbm.getList(key)
		lst := glst.lst
		rw := glst.rw
		rw.Lock()
		defer rw.Unlock()
		length := int64(lst.Len())
		if end < 0 {
			end = length + end
		}
		if start < 0 {
			start = length + start
		}
		if end < start {
			for i := int64(0); i < length; i++ {
				lst.Remove(lst.Front())
			}
			return nil
		}
		for i := int64(0); i < start; i++ {
			front := lst.Front()
			if front != nil {
				lst.Remove(front)
			} else {
				break
			}
		}
		for i := int64(0); i < length - end - 1; i++ {
			back := lst.Back()
			if back != nil {
				lst.Remove(back)
			} else {
				break
			}
		}
		return nil
	}
}

func (dbm *DBMock) LRange(key string, start int64, end int64) ([]string, error) {
	if !dbm.Exists(key) {
		return []string{}, nil
	} else if !dbm.isList(key) {
		return nil, turing.WrongTypeError
	} else {
		glst := dbm.getList(key)
		lst := glst.lst
		rw := glst.rw
		rw.RLock()
		defer rw.RUnlock()
		if end < 0 {
			end = int64(lst.Len()) + end
		}
		if start < 0 {
			start = int64(lst.Len()) + start
		}
		if start > end || start >= int64(lst.Len()) {
			return []string{}, nil
		}
		var c int64 = 0
		head := lst.Front()
		for c < start {
			head = head.Next()
			c++
		}
		res := []string{ head.Value.(string) }
		for c < end && head != nil {
			head = head.Next()
			if head != nil {
				res = append(res, head.Value.(string))
			}
			c++
		}
		return res, nil
	}
}

func (dbm *DBMock) Publish(key string, value interface{}) int {
	dbm.rw.RLock()
	defer dbm.rw.RUnlock()
	subs, ok := dbm.subs[key]
	if !ok {
		return 0
	}

	for _, sub := range subs {
		sub(key, value)
	}
	return len(subs)
}

func (dbm *DBMock) Subscribe(key string, fn func (key string, value interface{})) {
	dbm.rw.Lock()
	defer dbm.rw.Unlock()
	subs, ok := dbm.subs[key]
	if !ok {
		dbm.subs[key] = []func(key string, value interface{}){ fn }
	} else {
		subs = append(subs, fn)
		dbm.subs[key] = subs
	}
}

func (dbm *DBMock) LPop(key string) (string, error) {
	if !dbm.Exists(key) {
		return "", turing.KeyNotExistsError
	} else if !dbm.isList(key) {
		return "", turing.WrongTypeError
	} else {
		lst := dbm.getList(key)
		lst.rw.Lock()
		defer lst.rw.Unlock()
		if lst.lst.Len() == 0 {
			dbm.Delete(key)
			return "", turing.KeyNotExistsError
		} else {
			return lst.lst.Remove(lst.lst.Front()).(string), nil
		}
	}
}

func (dbm *DBMock) RPop(key string) (string, error) {
	if !dbm.Exists(key) {
		return "", turing.KeyNotExistsError
	} else if !dbm.isList(key) {
		return "", turing.WrongTypeError
	} else {
		lst := dbm.getList(key)
		lst.rw.Lock()
		defer lst.rw.Unlock()
		return lst.lst.Remove(lst.lst.Back()).(string), nil
	}
}

func (dbm *DBMock) IncreaseBy(key string, value float64) (float64, error) {
	s, err := dbm.Get(key)
	if err == turing.WrongTypeError {
		return 0, turing.WrongTypeError
	} else if err == turing.KeyNotExistsError {
		dbm.Set(key, strconv.FormatFloat(value, 'f', -1, 64))
		return value, nil
	} else {
		num, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, err
		}
		num += value
		dbm.Set(key, strconv.FormatFloat(num, 'f', -1, 64))
		return num, nil
	}
}

func NewDBMock() *DBMock {
	return &DBMock{
		turing.NewKVStoreMemory(),
		&dbMockInternal{
			subs: make(map[string][]func(key string, value interface{})),
		},
	}
}