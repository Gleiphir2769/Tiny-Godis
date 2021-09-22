package core

import (
	"Tiny-Godis/data_struct/dict"
	"Tiny-Godis/data_struct/lock"
)

func makeTestDB() *DB {
	return &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		locker:     lock.Make(lockerSize),
	}
}
