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
	union := sets[0].Union(sets[1])

	for i := 2; i < len(sets); i++ {
		union = sets[i].Union(union)
	}
	result := make([][]byte, union.Len())
	i := 0
	union.ForEach(func(key string, val interface{}) bool {
		result[i] = []byte(key)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

func execSUnionStore(db *DB, args [][]byte) redis.Reply {
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
	union := sets[0].Union(sets[1])
	for i := 2; i < len(sets); i++ {
		union = sets[i].Union(union)
	}

	db.PutEntity(dest, &DataEntity{Data: union})

	return reply.MakeIntReply(int64(union.Len()))
}

func execSDiff(db *DB, args [][]byte) redis.Reply {
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
	diff := sets[0].Diff(sets[1])

	for i := 2; i < len(sets); i++ {
		diff = sets[i].Diff(diff)
	}
	result := make([][]byte, diff.Len())
	i := 0
	diff.ForEach(func(key string, val interface{}) bool {
		result[i] = []byte(key)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

func execSDiffStore(db *DB, args [][]byte) redis.Reply {
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
	diff := sets[0].Diff(sets[1])
	for i := 2; i < len(sets); i++ {
		diff = sets[i].Diff(diff)
	}

	db.PutEntity(dest, &DataEntity{Data: diff})

	return reply.MakeIntReply(int64(diff.Len()))
}

// undoSetChange rollbacks SADD and SREM command
func undoSetChange(db *DB, args [][]byte) []CmdLine {
	key := args[0]
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i])
	}
	return rollbackSetMembers(db, string(key), members...)
}

// prepareSetCalculate return which sets will be calculated
func prepareSetCalculate(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	return nil, keys
}

// prepareSetCalculateStore return the store destination and sets will be calculated
func prepareSetCalculateStore(args [][]byte) ([]string, []string) {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}
	return []string{dest}, keys
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
