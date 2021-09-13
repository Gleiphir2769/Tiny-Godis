package Tiny_Godis

import (
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/redis/reply"
	"strconv"
	"strings"
	"time"
)

func (db *DB) GetAsString(key string) ([]byte, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	value, ok := entity.Data.([]byte)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return value, nil
}

func execGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	val, err := db.GetAsString(key)
	if err != nil {
		return err
	}
	if val == nil {
		return &reply.NullBulkReply{}
	}
	return reply.MakeBulkReply(val)
}

const unlimitedTTL int64 = 0

const (
	upsertPolicy = iota
	insertPolicy
	updatePolicy
)

func execSet(db *DB, args [][]byte) redis.Reply {
	key := args[0]
	val := args[1]
	policy := upsertPolicy
	ttl := unlimitedTTL

	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			switch strings.ToUpper(string(args[i])) {
			case "NX":
				if policy == updatePolicy {
					return &reply.SyntaxErrReply{}
				}
				policy = insertPolicy
			case "XX":
				if policy == insertPolicy {
					return &reply.SyntaxErrReply{}
				}
				policy = upsertPolicy
			case "EX":
				if ttl != unlimitedTTL || i+1 > len(args) {
					return &reply.SyntaxErrReply{}
				}
				temp := args[i+1]
				expireTime, err := strconv.ParseInt(string(temp), 10, 64)
				if err != nil {
					return &reply.SyntaxErrReply{}
				}
				if expireTime < 0 {
					return reply.MakeErrReply("Err Invalid expire time to set")
				}
				ttl = expireTime * 1000
				i++
			case "PX":
				if ttl != unlimitedTTL || i+1 > len(args) {
					return &reply.SyntaxErrReply{}
				}
				temp := args[i+1]
				expireTime, err := strconv.ParseInt(string(temp), 10, 64)
				if err != nil {
					return &reply.SyntaxErrReply{}
				}
				if expireTime < 0 {
					return reply.MakeErrReply("Err Invalid expire time to set")
				}
				ttl = expireTime
				i++
			default:
				return &reply.SyntaxErrReply{}
			}
		}
	}

	entity := DataEntity{Data: val}
	var result int
	db.Persist(string(key))
	switch policy {
	case upsertPolicy:
		result = db.PutEntity(string(key), &entity)
	case insertPolicy:
		result = db.PutIfAbsent(string(key), &entity)
	case updatePolicy:
		result = db.PutIfExists(string(key), &entity)
	}

	if ttl != unlimitedTTL {
		expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
		db.Expire(string(key), expireTime)
		// todo：等待aof相关操作
	} else if result > 0 {
		db.Persist(string(key))
		// todo：等待aof相关操作
	} else {
		// todo：等待aof相关操作
	}

	if policy == upsertPolicy || result > 0 {
		return &reply.OkReply{}
	}
	return &reply.NullBulkReply{}
}

func init() {
	RegisterCommand("Set", execSet, writeFirstKey, rollbackFirstKey, -3)
	RegisterCommand("Get", execGet, readFirstKey, nil, 2)
}
