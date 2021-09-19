package Tiny_Godis

import (
	"Tiny-Godis/data_struct/set"
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/redis/reply"
)

func (db *DB) getAsSet(key string) (*set.Set, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	value, ok := entity.Data.(*set.Set)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return value, nil
}

func (db *DB) getOrInitSet(key string) (*set.Set, reply.ErrorReply) {
	s, errReply := db.getAsSet(key)
	if errReply != nil {
		return nil, errReply
	}
	if s == nil {
		s = set.MakeSet()
		e := DataEntity{Data: s}
		db.PutEntity(key, &e)
	}
	return s, nil
}

func execSAdd(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	members := args[1:]
	result := 0
	s, errReply := db.getOrInitSet(key)
	if errReply != nil {
		return errReply
	}
	for _, m := range members {
		result += s.Add(string(m))
	}
	return reply.MakeIntReply(int64(result))
}

func execSIsMember(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	member := args[1]
	s, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return reply.MakeIntReply(0)
	}
	if s.Has(string(member)) {
		return reply.MakeIntReply(1)
	}
	return reply.MakeIntReply(0)
}

func execSRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	members := args[1:]
	s, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return reply.MakeIntReply(0)
	}
	result := 0
	for _, member := range members {
		result += s.Remove(string(member))
	}
	if s.Len() == 0 {
		db.Remove(key)
	}
	return reply.MakeIntReply(int64(result))
}

func execSCard(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	s, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return reply.MakeIntReply(0)
	}
	return reply.MakeIntReply(int64(s.Len()))
}

func execSMembers(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	s, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return reply.MakeNullBulkReply()
	}
	result := make([][]byte, s.Len())
	i := 0
	s.ForEach(func(key string, val interface{}) bool {
		result[i] = []byte(key)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

// todo: test
func execSInter(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	sets := make([]*set.Set, len(keys))
	for _, key := range keys {
		s, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if s == nil {
			return &reply.EmptyMultiBulkReply{}
		}
		sets = append(sets, s)
	}
	if len(sets) <= 2 {
		return &reply.EmptyMultiBulkReply{}
	}
	inter := sets[0].Intersect(sets[1])
	for i := 2; i < len(sets); i++ {
		inter = sets[i].Intersect(inter)
	}
	result := make([][]byte, inter.Len())
	i := 0
	inter.ForEach(func(key string, val interface{}) bool {
		result[i] = []byte(key)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

func execSInterStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	sets := make([]*set.Set, len(keys))
	for _, key := range keys {
		s, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if s == nil {
			return reply.MakeIntReply(0)
		}
		sets = append(sets, s)
	}
	if len(sets) <= 2 {
		return reply.MakeIntReply(0)
	}
	inter := sets[0].Intersect(sets[1])
	for i := 2; i < len(sets); i++ {
		inter = sets[i].Intersect(inter)
	}

	db.PutEntity(dest, &DataEntity{Data: inter})

	return reply.MakeIntReply(int64(inter.Len()))
}

func execSUnion(db *DB, args [][]byte) redis.Reply {
}

func execSUnionStore(db *DB, args [][]byte) redis.Reply {
}

func execSDiff(db *DB, args [][]byte) redis.Reply {
}

func execSDiffStore(db *DB, args [][]byte) redis.Reply {
}

func undoSetChange(db *DB, args [][]byte) []CmdLine {

}

func prepareSetCalculate(args [][]byte) ([]string, []string) {

}

func prepareSetCalculateStore(args [][]byte) ([]string, []string) {

}

func init() {
	RegisterCommand("SAdd", execSAdd, writeFirstKey, undoSetChange, -3)
	RegisterCommand("SIsMember", execSIsMember, readFirstKey, nil, 3)
	RegisterCommand("SRem", execSRem, writeFirstKey, undoSetChange, -3)
	RegisterCommand("SCard", execSCard, readFirstKey, nil, 2)
	RegisterCommand("SMembers", execSMembers, readFirstKey, nil, 2)
	RegisterCommand("SInter", execSInter, prepareSetCalculate, nil, -2)
	RegisterCommand("SInterStore", execSInterStore, prepareSetCalculateStore, rollbackFirstKey, -3)
	RegisterCommand("SUnion", execSUnion, prepareSetCalculate, nil, -2)
	RegisterCommand("SUnionStore", execSUnionStore, prepareSetCalculateStore, rollbackFirstKey, -3)
	RegisterCommand("SDiff", execSDiff, prepareSetCalculate, nil, -2)
	RegisterCommand("SDiffStore", execSDiffStore, prepareSetCalculateStore, rollbackFirstKey, -3)
}
