package Tiny_Godis

import (
	"Tiny-Godis/data_struct/dict"
	"Tiny-Godis/data_struct/lock"
	"sync"
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
}

type DataEntity struct {
	Data interface{}
}

func MakeDB() *DB {
	db := DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(lockerSize),
		locker:     lock.Make(lockerSize),
	}

	return &db
}

func (db *DB) GetEntity(key string) (*DataEntity, bool) {
	db.stopWait.Wait()

	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	// todo: 判断过期
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

	// todo: ttl 相关处理

	return r1
}

func (db *DB) Removes(keys ...string) (deleted int) {
	db.stopWait.Wait()
	for _, key := range keys {
		r := db.Remove(key)

		// todo: ttl 相关处理

		if r == 1 {
			deleted++
		}
	}
	return deleted
}
