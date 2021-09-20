package Tiny_Godis

import (
	"Tiny-Godis/data_struct/list"
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/lib/utils"
	"Tiny-Godis/redis/reply"
	"fmt"
	"strconv"
)

func (db *DB) getAsList(key string) (*list.LinkedList, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	value, ok := entity.Data.(*list.LinkedList)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return value, nil
}

func (db *DB) getOrInitList(key string) (*list.LinkedList, reply.ErrorReply) {
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return nil, errReply
	}
	if ll == nil {
		ll = list.MakeLinkedList()
		e := DataEntity{Data: ll}
		db.PutEntity(key, &e)
		return ll, nil
	}
	return ll, nil
}

func execLPush(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]
	ll, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}
	for _, v := range values {
		ll.LPush(v)
	}
	return reply.MakeIntReply(int64(ll.Len()))
}

func execLPushX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ll == nil {
		return reply.MakeIntReply(int64(0))
	}
	for _, v := range values {
		ll.LPush(v)
	}
	return reply.MakeIntReply(int64(ll.Len()))
}

func execRPush(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]
	ll, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}
	for _, v := range values {
		ll.RPush(v)
	}
	return reply.MakeIntReply(int64(ll.Len()))
}

func execRPushX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ll == nil {
		return reply.MakeIntReply(int64(0))
	}
	for _, v := range values {
		ll.RPush(v)
	}
	return reply.MakeIntReply(int64(ll.Len()))
}

func execLPop(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ll == nil {
		return reply.MakeNullBulkReply()
	}
	raw := ll.LPop()
	v, _ := raw.([]byte)
	if ll.Len() == 0 {
		db.Remove(key)
	}
	return reply.MakeBulkReply(v)
}

func execRPop(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ll == nil {
		return reply.MakeNullBulkReply()
	}
	raw := ll.RPop()
	v, _ := raw.([]byte)
	if ll.Len() == 0 {
		db.Remove(key)
	}
	return reply.MakeBulkReply(v)
}

func execRPopLPush(db *DB, args [][]byte) redis.Reply {
	sourceKey := string(args[0])
	desKey := string(args[1])
	sll, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return errReply
	}
	dll, errReply := db.getOrInitList(desKey)
	if errReply != nil {
		return errReply
	}
	if sll == nil {
		return reply.MakeNullBulkReply()
	}
	v := sll.RPop().([]byte)
	dll.LPush(v)
	if sll.Len() == 0 {
		db.Remove(sourceKey)
	}
	return reply.MakeBulkReply(v)
}

func execLRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	count := int(count64)
	value := args[2]
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ll == nil {
		return reply.MakeIntReply(0)
	}

	removed := 0
	if count == 0 {
		removed = ll.RemoveAllByVal(value)
	} else if count > 0 {
		removed = ll.RemoveByVal(value, count)
	} else {
		removed = ll.ReverseRemove(value, count)
	}

	if ll.Len() == 0 {
		db.Remove(key)
	}

	return reply.MakeIntReply(int64(removed))
}

func execLLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ll == nil {
		return reply.MakeIntReply(0)
	}

	return reply.MakeIntReply(int64(ll.Len()))
}

func execLIndex(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	index := int(index64)
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ll == nil {
		return reply.MakeNullBulkReply()
	}

	if index >= ll.Len() || index < -ll.Len() {
		return reply.MakeErrReply(fmt.Sprintf("ERR index '%d' out of range '%d'", index, ll.Len()))
	} else if index < 0 && index >= -ll.Len() {
		index = index + ll.Len()
	}

	v, _ := ll.Get(index).([]byte)
	return reply.MakeBulkReply(v)
}

func execLSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	index := int(index64)
	value := args[2]
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ll == nil {
		return reply.MakeErrReply("ERR no such key")
	}

	if index >= ll.Len() || index < -ll.Len() {
		return reply.MakeErrReply(fmt.Sprintf("ERR index '%d' out of range '%d'", index, ll.Len()))
	} else if index < 0 && index >= -ll.Len() {
		index = index + ll.Len()
	}

	ll.Set(index, value)
	return &reply.OkReply{}
}

func execLRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	start := int(start64)
	stop := int(stop64)
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ll == nil {
		return reply.MakeErrReply("ERR no such key")
	}

	// compute index
	size := ll.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &reply.EmptyMultiBulkReply{}
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	slice := ll.Range(start, stop)
	result := make([][]byte, len(slice))
	for i := 0; i < len(slice); i++ {
		result[i] = slice[i].([]byte)
	}

	return reply.MakeMultiBulkReply(result)
}

func undoRPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	count := len(args) - 1
	cmdLines := make([]CmdLine, count)
	for i := 0; i < count; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("RPOP", key))
	}
	return cmdLines
}

func undoLPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	count := len(args) - 1
	cmdLines := make([]CmdLine, count)
	for i := 0; i < count; i++ {
		cmdLines[i] = utils.ToCmdLine("LPOP", key)
	}
	return cmdLines
}

func undoLPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if ll == nil {
		return nil
	}
	v, _ := ll.Get(0).([]byte)
	cmdLines := make([]CmdLine, 1)
	cmdLines[0] = utils.ToCmdLine("LPUSH", key, string(v))
	return cmdLines
}

func undoRPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if ll == nil {
		return nil
	}
	v, _ := ll.Get(0).([]byte)
	cmdLines := make([]CmdLine, 1)
	cmdLines[0] = utils.ToCmdLine("RPUSH", key, string(v))
	return cmdLines
}

var rPushCmd = []byte("RPUSH")

func undoRPopLPush(db *DB, args [][]byte) []CmdLine {
	sourceKey := string(args[0])
	ll, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return nil
	}
	if ll == nil || ll.Len() == 0 {
		return nil
	}
	element, _ := ll.Get(ll.Len() - 1).([]byte)
	return []CmdLine{
		{
			rPushCmd,
			args[0],
			element,
		},
		{
			[]byte("LPOP"),
			args[1],
		},
	}
}

func undoLSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil
	}
	index := int(index64)
	ll, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if ll == nil {
		return nil
	}
	size := ll.Len() // assert: size > 0
	if index < -1*size {
		return nil
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return nil
	}
	value, _ := ll.Get(index).([]byte)
	return []CmdLine{
		{
			[]byte("LSET"),
			args[0],
			args[1],
			value,
		},
	}
}

func prepareRPopLPush(args [][]byte) ([]string, []string) {
	return []string{
		string(args[0]),
		string(args[1]),
	}, nil
}

func init() {
	RegisterCommand("LPush", execLPush, writeFirstKey, undoLPush, -3)
	RegisterCommand("LPushX", execLPushX, writeFirstKey, undoLPush, -3)
	RegisterCommand("RPush", execRPush, writeFirstKey, undoRPush, -3)
	RegisterCommand("RPushX", execRPushX, writeFirstKey, undoRPush, -3)
	RegisterCommand("LPop", execLPop, writeFirstKey, undoLPop, 2)
	RegisterCommand("RPop", execRPop, writeFirstKey, undoRPop, 2)
	RegisterCommand("RPopLPush", execRPopLPush, prepareRPopLPush, undoRPopLPush, 3)
	RegisterCommand("LRem", execLRem, writeFirstKey, rollbackFirstKey, 4)
	RegisterCommand("LLen", execLLen, readFirstKey, nil, 2)
	RegisterCommand("LIndex", execLIndex, readFirstKey, nil, 3)
	RegisterCommand("LSet", execLSet, writeFirstKey, undoLSet, 4)
	RegisterCommand("LRange", execLRange, readFirstKey, nil, 4)
}
