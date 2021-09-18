package dict

import (
	"math"
	"sync"
	"sync/atomic"
)

type ConcurrentDict struct {
	table []*Shard
	count int32
}

type Shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	} else {
		return int(n + 1)
	}
}

func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{
			m: make(map[string]interface{}),
		}
	}
	d := &ConcurrentDict{
		count: 0,
		table: table,
	}
	return d
}

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// todo: 为什么hashcode是2的整数幂
func (dict *ConcurrentDict) spread(hashcode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	dictSize := uint32(len(dict.table))
	// 定位shard, 当n为2的整数幂时 h % n == (n - 1) & h
	return (dictSize - 1) & hashcode
}

func (dict *ConcurrentDict) getShared(index uint32) *Shard {
	return dict.table[index]
}

func (dict *ConcurrentDict) addCount() {
	atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() {
	atomic.AddInt32(&dict.count, -1)
}

func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	hashcode := fnv32(key)
	index := dict.spread(hashcode)
	shared := dict.getShared(index)
	shared.mutex.Lock()
	defer shared.mutex.Unlock()

	val, exists = shared.m[key]
	return
}

func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashcode := fnv32(key)
	index := dict.spread(hashcode)
	shared := dict.getShared(index)
	shared.mutex.Lock()
	defer shared.mutex.Unlock()

	if _, ok := shared.m[key]; ok {
		shared.m[key] = val
		return 0
	} else {
		shared.m[key] = val
		dict.addCount()
		return 1
	}
}

func (dict *ConcurrentDict) Len() (length int) {
	for _, s := range dict.table {
		length += len(s.m)
	}
	return length
}

// PutIfAbsent if the key has existed, the value will not be replaced.
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashcode := fnv32(key)
	index := dict.spread(hashcode)
	shared := dict.getShared(index)
	shared.mutex.Lock()
	defer shared.mutex.Unlock()

	if _, ok := shared.m[key]; ok {
		return 0
	} else {
		shared.m[key] = val
		dict.addCount()
		return 1
	}
}

// PutIfExists the value will only be put when key has existed
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashcode := fnv32(key)
	index := dict.spread(hashcode)
	shared := dict.getShared(index)
	shared.mutex.Lock()
	defer shared.mutex.Unlock()

	if _, ok := shared.m[key]; ok {
		shared.m[key] = val
		return 1
	} else {
		return 0
	}
}

func (dict *ConcurrentDict) Remove(key string) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashcode := fnv32(key)
	index := dict.spread(hashcode)
	shared := dict.getShared(index)
	shared.mutex.Lock()
	defer shared.mutex.Unlock()

	if _, ok := shared.m[key]; ok {
		delete(shared.m, key)
		dict.decreaseCount()
		return 1
	} else {
		return 0
	}
}

func (dict *ConcurrentDict) ForEach(recall RecallFunc) {
	if dict == nil {
		return
	}
	for _, t := range dict.table {
		// 这段加锁再释放的代码很精彩，灵活运用了匿名函数
		t.mutex.RLock()
		func() {
			defer t.mutex.RUnlock()
			for k, v := range t.m {
				if recall(k, v) {
					return
				}
			}
		}()
	}
}
