package core

import (
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/redis/reply"
	"strconv"
	"time"
)

func execDel(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, k := range args {
		keys[i] = string(k)
	}

	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.AddAof(makeAofCmd("DEL", args))
	}
	return reply.MakeIntReply(int64(deleted))
}

func undoDel(db *DB, args [][]byte) []CmdLine {
	keys := make([]string, len(args))
	for i, k := range args {
		keys[i] = string(k)
	}
	return rollbackGivenKeys(db, keys...)
}

func execExists(db *DB, args [][]byte) redis.Reply {
	result := 0
	for _, k := range args {
		key := string(k)
		if _, ok := db.GetEntity(key); ok {
			result++
		}
	}
	return reply.MakeIntReply(int64(result))
}

func execExpire(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	expireTime := time.Duration(ttl) * time.Second
	_, exist := db.GetEntity(key)
	if !exist {
		return reply.MakeIntReply(0)
	}
	expireAt := time.Now().Add(expireTime)
	db.Expire(key, expireAt)
	db.AddAof(makeExpireAofCmd(key, expireAt))
	return reply.MakeIntReply(1)
}

func execExpireAt(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	expireAt := time.Unix(raw, 0)
	_, exist := db.GetEntity(key)
	if !exist {
		return reply.MakeIntReply(0)
	}
	db.Expire(key, expireAt)
	db.AddAof(makeExpireAofCmd(key, expireAt))
	return reply.MakeIntReply(1)
}

func execPExpire(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	expireTime := time.Duration(ttl) * time.Millisecond
	_, exist := db.GetEntity(key)
	if !exist {
		return reply.MakeIntReply(0)
	}
	expireAt := time.Now().Add(expireTime)
	db.Expire(key, expireAt)
	db.AddAof(makeExpireAofCmd(key, expireAt))
	return reply.MakeIntReply(1)
}

func execPExpireAt(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	expireAt := time.Unix(0, raw*int64(time.Millisecond))
	_, exist := db.GetEntity(key)
	if !exist {
		return reply.MakeIntReply(0)
	}
	db.Expire(key, expireAt)
	db.AddAof(makeExpireAofCmd(key, expireAt))
	return reply.MakeIntReply(1)
}

func execTTL(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exist := db.GetEntity(key)
	if !exist {
		return reply.MakeIntReply(-2)
	}
	raw, exist := db.ttlMap.Get(key)
	if !exist {
		return reply.MakeIntReply(-1)
	}

	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return reply.MakeIntReply(int64(ttl / time.Second))
}

func execPersist(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exist := db.GetEntity(key)
	if !exist {
		return reply.MakeIntReply(0)
	}
	_, exist = db.ttlMap.Get(key)
	if !exist {
		return reply.MakeIntReply(0)
	}
	db.Persist(key)
	db.AddAof(makeAofCmd("PERSIST", args))
	return reply.MakeIntReply(1)
}

// BGRewriteAOF asynchronously rewrites Append-Only-File
func BGRewriteAOF(db *DB, args [][]byte) redis.Reply {
	go db.RewriteAof()
	return reply.MakeStatusReply("Background append only file rewriting started")
}

func init() {
	RegisterCommand("Del", execDel, writeAllKeys, undoDel, -2)
	RegisterCommand("Exists", execExists, readAllKeys, nil, -2)
	RegisterCommand("Expire", execExpire, writeFirstKey, nil, 3)
	RegisterCommand("ExpireAt", execExpireAt, writeFirstKey, nil, 3)
	RegisterCommand("PExpire", execPExpire, writeFirstKey, nil, 3)
	RegisterCommand("PExpireAt", execPExpireAt, writeFirstKey, nil, 3)
	RegisterCommand("TTL", execTTL, readFirstKey, nil, 2)
	RegisterCommand("Persist", execPersist, writeFirstKey, nil, 2)
}
