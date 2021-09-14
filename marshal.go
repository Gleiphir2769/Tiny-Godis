package Tiny_Godis

import (
	"Tiny-Godis/lib/utils"
	"Tiny-Godis/redis/reply"
	"strconv"
	"time"
)

func EntityToSetCmd(key string, entity *DataEntity) *reply.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *reply.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToSetCmd(key, val)
		return cmd
	default:
		return cmd
	}
}

var setCmd = []byte("SET")

func stringToSetCmd(key string, data []byte) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = data
	return reply.MakeMultiBulkReply(args)
}

// toTTLCmd serialize ttl config
func toTTLCmd(db *DB, key string) *reply.MultiBulkReply {
	raw, exists := db.ttlMap.Get(key)
	if !exists {
		// 无 TTL
		return reply.MakeMultiBulkReply(utils.ToCmdLine("PERSIST", key))
	}
	expireTime, _ := raw.(time.Time)
	timestamp := strconv.FormatInt(expireTime.UnixNano()/1000/1000, 10)
	return reply.MakeMultiBulkReply(utils.ToCmdLine("PEXPIREAT", key, timestamp))
}
