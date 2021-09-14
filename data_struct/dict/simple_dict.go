package dict

import (
	"sync"
	"sync/atomic"
)

type SimpleDict struct {
	table map[string]interface{}
	mu    sync.Mutex
	count int32
}

func MakeSimpleDict() *SimpleDict {
	return &SimpleDict{table: make(map[string]interface{})}
}

func (sd *SimpleDict) addCount() {
	atomic.AddInt32(&sd.count, 1)
}

func (sd *SimpleDict) decreaseCount() {
	atomic.AddInt32(&sd.count, -1)
}

func (sd *SimpleDict) Put(key string, val interface{}) (result int) {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	if _, ok := sd.table[key]; ok {
		sd.table[key] = val
		return 0
	} else {
		sd.table[key] = val
		sd.addCount()
		return 1
	}
}

func (sd *SimpleDict) Get(key string) (val interface{}, exists bool) {
	val, exists = sd.table[key]
	return
}

func (sd *SimpleDict) Len() int {
	return len(sd.table)
}

func (sd *SimpleDict) PutIfAbsent(key string, val interface{}) (result int) {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	if _, ok := sd.table[key]; ok {
		return 0
	} else {
		sd.table[key] = val
		sd.addCount()
		return 1
	}
}

func (sd *SimpleDict) PutIfExists(key string, val interface{}) (result int) {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	if _, ok := sd.table[key]; ok {
		sd.table[key] = val
		return 1
	} else {
		return 0
	}
}

func (sd *SimpleDict) Remove(key string) (result int) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if _, ok := sd.table[key]; ok {
		delete(sd.table, key)
		sd.decreaseCount()
		return 1
	} else {
		return 0
	}
}
