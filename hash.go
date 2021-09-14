package Tiny_Godis

import (
	"Tiny-Godis/data_struct/dict"
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/redis/reply"
)

func (db *DB) getAsDict(key string) (dict.Dict, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	value, ok := entity.Data.(dict.Dict)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return value, nil
}

func (db *DB) getOrInitDict(key string) (dict.Dict, reply.ErrorReply) {
	d, result := db.getAsDict(key)
	if result != nil {
		return nil, result
	}
	if d == nil {
		nd := dict.MakeSimpleDict()
		entity := DataEntity{Data: nd}
		db.PutEntity(key, &entity)
		return nd, nil
	}
	return d, nil
}

func execHSet(db *DB, args [][]byte) redis.Reply {
	key := args[0]
	field := args[1]
	val := args[2]

	d, err := db.getOrInitDict(string(key))
	if err != nil {
		return err
	}
	result := d.Put(string(field), val)
	return reply.MakeIntReply(int64(result))
}

func undoHSet(db *DB, args [][]byte) []CmdLine {
	key := args[0]
	field := args[1]
	return rollbackHashFields(db, string(key), string(field))
}

func execHSetNx(db *DB, args [][]byte) redis.Reply {
	key := args[0]
	field := args[1]
	val := args[2]

	d, err := db.getOrInitDict(string(key))
	if err != nil {
		return err
	}
	result := d.PutIfAbsent(string(field), val)
	return reply.MakeIntReply(int64(result))
}
