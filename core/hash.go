package core

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

func execHSetNX(db *DB, args [][]byte) redis.Reply {
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

func execHExists(db *DB, args [][]byte) redis.Reply {
	key := args[0]
	field := args[1]

	d, err := db.getAsDict(string(key))
	if err != nil {
		return err
	} else if d == nil {
		return reply.MakeIntReply(0)
	}

	_, ok := d.Get(string(field))
	if ok {
		return reply.MakeIntReply(1)
	}
	return reply.MakeIntReply(0)
}

func execHDel(db *DB, args [][]byte) redis.Reply {
	key := args[0]
	fields := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = args[i]
	}

	d, err := db.getAsDict(string(key))
	if err != nil {
		return err
	} else if d == nil {
		return reply.MakeIntReply(0)
	}

	deleted := 0
	for _, field := range fields {
		r := d.Remove(string(field))
		deleted += r
	}

	if d.Len() == 0 {
		db.Remove(string(key))
	}

	if deleted > 0 {
		//todo: aof
	}

	return reply.MakeIntReply(int64(deleted))
}

func undoHDel(db *DB, args [][]byte) []CmdLine {
	key := args[0]
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = string(args[i])
	}

	return rollbackHashFields(db, string(key), fields...)
}

func execHLen(db *DB, args [][]byte) redis.Reply {
	key := args[0]

	d, err := db.getAsDict(string(key))
	if err != nil {
		return err
	} else if d == nil {
		return reply.MakeIntReply(0)
	}

	return reply.MakeIntReply(int64(d.Len()))
}

func execHGet(db *DB, args [][]byte) redis.Reply {
	key := args[0]
	field := args[1]

	d, err := db.getAsDict(string(key))
	if err != nil {
		return err
	} else if d == nil {
		return &reply.NullBulkReply{}
	}

	raw, ok := d.Get(string(field))
	if !ok {
		return &reply.NullBulkReply{}
	}
	value, _ := raw.([]byte)
	return reply.MakeBulkReply(value)
}

func init() {
	RegisterCommand("HSet", execHSet, writeFirstKey, undoHSet, 4)
	RegisterCommand("HSetNX", execHSetNX, writeFirstKey, undoHSet, 4)
	RegisterCommand("HGet", execHGet, readFirstKey, nil, 3)
	RegisterCommand("HExists", execHExists, readFirstKey, nil, 3)
	RegisterCommand("HDel", execHDel, writeFirstKey, undoHDel, -3)
	RegisterCommand("HLen", execHLen, readFirstKey, nil, 2)
}
