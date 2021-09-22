package Tiny_Godis

import (
	"Tiny-Godis/data_struct/dict"
	"Tiny-Godis/data_struct/lock"
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/lib/config"
	"Tiny-Godis/lib/logger"
	"Tiny-Godis/lib/timewheel"
	"Tiny-Godis/redis/reply"
	"os"
	"sync"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockerSize   = 1024
	aofQueueSize = 1 << 16
)

type DB struct {
	data       dict.Dict
	ttlMap     dict.Dict
	versionMap dict.Dict

	locker *lock.Locks

	stopWait sync.WaitGroup

	aofChan     chan *reply.MultiBulkReply
	aofFile     *os.File
	aofFileName string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shutdown
	aofFinished chan struct{}
	// buffer commands received during aof rewrite progress
	aofRewriteBuffer chan *reply.MultiBulkReply
	aofPause         sync.RWMutex
}

type DataEntity struct {
	Data interface{}
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
type PreFunc func(args [][]byte) ([]string, []string)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// UndoFunc returns undo logs for the given command line
// execute from head to tail when undo
// warning: undoFunc并不是真的回滚，而是返回一个回滚的命令序列给用户，让用户再去执行回滚的命令序列
type UndoFunc func(db *DB, args [][]byte) []CmdLine

func MakeDB() *DB {
	db := DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(lockerSize),
		locker:     lock.Make(lockerSize),
	}

	if config.Properties.AppendOnly {
		db.aofFileName = config.Properties.AppendFilename
		db.loadAof(0)
		f, err := os.OpenFile(db.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			logger.Warn(err)
		} else {
			db.aofFile = f
			db.aofChan = make(chan *reply.MultiBulkReply, aofQueueSize)
		}
		db.aofFinished = make(chan struct{})
		go func() {
			db.handleAof()
		}()
	}

	return &db
}

func MakeTmpDB() *DB {
	db := DB{
		data:   dict.MakeSimpleDict(),
		ttlMap: dict.MakeSimpleDict(),
		locker: lock.Make(lockerSize),
	}

	return &db
}

/* ---- Main Function ----- */

func (db *DB) GetEntity(key string) (*DataEntity, bool) {
	db.stopWait.Wait()

	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*DataEntity)
	return entity, true
}

func (db *DB) PutEntity(key string, value *DataEntity) int {
	db.stopWait.Wait()
	return db.data.Put(key, value)
}

func (db *DB) PutIfExists(key string, value *DataEntity) int {
	db.stopWait.Wait()
	return db.data.PutIfExists(key, value)
}

func (db *DB) PutIfAbsent(key string, value *DataEntity) int {
	db.stopWait.Wait()
	return db.data.PutIfAbsent(key, value)
}

func (db *DB) Remove(key string) (result int) {
	db.stopWait.Wait()
	r1 := db.data.Remove(key)
	db.ttlMap.Remove(key)
	timewheel.Cancel(key)
	return r1
}

func (db *DB) Removes(keys ...string) (deleted int) {
	db.stopWait.Wait()
	for _, key := range keys {
		r := db.Remove(key)
		if r == 1 {
			deleted++
		}
	}
	return deleted
}

/* ---- Lock Function ----- */

// Lock lock key for writing
func (db *DB) Lock(key string) {
	db.locker.Lock(key)
}

// UnLock releases key for writing
func (db *DB) UnLock(key string) {
	db.locker.UnLock(key)
}

// RWLocks lock keys for writing and reading
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}

/* ---- TTL Function ----- */

// Expire set ttlcmd
func (db *DB) Expire(key string, expireTime time.Time) {
	db.stopWait.Wait()
	db.ttlMap.Put(key, expireTime)

	timewheel.At(expireTime, key, func() {
		db.Lock(key)
		defer db.UnLock(key)
		logger.Info("expire " + key)
		// double check: 因为等待落锁的过程中ttlmap可能已被改变（key 的过期事件发生变化，上锁的目的亦在此）（第一次check是由timeWheel完成的）
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}
		ret := rawExpireTime.(time.Time)
		expired := time.Now().After(ret)
		if expired {
			db.Remove(key)
		}
	})
}

func (db *DB) Persist(key string) {
	db.stopWait.Wait()
	db.ttlMap.Remove(key)
	timewheel.Cancel(key)
}

// IsExpired check whether a key is expired (不直接查看db中是否有这个键来判断过期是因为时间轮并不保证精确过期)
func (db *DB) IsExpired(key string) bool {
	// 不直接查看db中是否有这个键来判断过期是因为时间轮并不保证精确过期
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

// Flush clean database
func (db *DB) Flush() {
	db.stopWait.Add(1)
	defer db.stopWait.Done()

	db.data = dict.MakeConcurrent(dataDictSize)
	db.ttlMap = dict.MakeConcurrent(ttlDictSize)
	db.locker = lock.Make(lockerSize)

}

/* ---- Version Function ----- */
func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		version := db.getVersion(key)
		db.versionMap.Put(key, version+1)
	}
}

func (db *DB) getVersion(key string) uint32 {
	v, ok := db.versionMap.Get(key)
	if ok {
		return v.(uint32)
	}
	return 0
}

/* ---- Util Function ----- */

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- Clean Function ----- */

func (db *DB) Close() {

}

func (db *DB) AfterClientClose(c redis.Connection) {

}
