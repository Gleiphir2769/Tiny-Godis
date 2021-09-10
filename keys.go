package Tiny_Godis

import (
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/redis/reply"
)

func execDel(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, k := range args {
		keys[i] = string(k)
	}

	deleted := db.Removes(keys...)
	if deleted > 0 {
		// todo: 等待aof支持
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
