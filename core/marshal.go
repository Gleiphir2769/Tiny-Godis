package core

import (
	"Tiny-Godis/data_struct/dict"
	"Tiny-Godis/data_struct/list"
	"Tiny-Godis/data_struct/set"
	"Tiny-Godis/lib/utils"
	"Tiny-Godis/redis/reply"
	"strconv"
	"time"
)

func EntityToCmd(key string, entity *DataEntity) *reply.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *reply.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToCmd(key, val)
		return cmd
	case list.List:
		cmd = listToCmd(key, val)
	case dict.Dict:
		cmd = hashToCmd(key, val)
	case *set.Set:
		cmd = setToCmd(key, val)
	}
	return cmd
}

var setCmd = []byte("SET")

func stringToCmd(key string, data []byte) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = data
	return reply.MakeMultiBulkReply(args)
}

var rpushAllCmd = []byte("RPUSH")

func listToCmd(key string, l list.List) *reply.MultiBulkReply {
	args := make([][]byte, 2+l.Len())
	args[0] = rpushAllCmd
	args[1] = []byte(key)
	index := 2
	l.ForEach(func(val interface{}) bool {
		bytes, _ := val.([]byte)
		args[index] = bytes
		index++
		return true
	})
	return reply.MakeMultiBulkReply(args)
}

var hsetCmd = []byte("HSET")

func hashToCmd(key string, d dict.Dict) *reply.MultiBulkReply {
	args := make([][]byte, 2+2*d.Len())
	args[0] = hsetCmd
	args[1] = []byte(key)
	index := 2
	d.ForEach(func(key string, val interface{}) bool {
		args[index] = []byte(key)
		bytes, _ := val.([]byte)
		args[index+1] = bytes
		index += 2
		return true
	})
	return reply.MakeMultiBulkReply(args)
}

var saddCmd = []byte("SADD")

func setToCmd(key string, s *set.Set) *reply.MultiBulkReply {
	args := make([][]byte, 2+s.Len())
	args[0] = saddCmd
	args[1] = []byte(key)
	index := 2
	s.ForEach(func(key string, val interface{}) bool {
		args[index] = []byte(key)
		index++
		return true
	})
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
