package pubsub

import (
	"Tiny-Godis/data_struct/list"
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/redis/reply"
)

func Publish(subs *SubPool, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return &reply.ArgNumErrReply{Cmd: "publish"}
	}
	channel := string(args[0])
	message := args[1]

	raw, ok := subs.pool.Get(channel)
	if !ok {
		return reply.MakeIntReply(0)
	}

	pubs, _ := raw.(*list.LinkedList)
	pubs.ForEach(func(raw interface{}) bool {
		conn, _ := raw.(redis.Connection)
		replyArgs := make([][]byte, 3)
		replyArgs[0] = messageBytes
		replyArgs[1] = []byte(channel)
		replyArgs[2] = message
		_ = conn.Write(reply.MakeMultiBulkReply(replyArgs).ToBytes())
		return true
	})

	return reply.MakeIntReply(int64(pubs.Len()))
}
