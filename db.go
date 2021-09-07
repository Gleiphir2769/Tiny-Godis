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

	stopWait *sync.WaitGroup
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
