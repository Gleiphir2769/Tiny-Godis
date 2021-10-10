package pubsub

import (
	"Tiny-Godis/data_struct/list"
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/redis/reply"
	"strconv"
)

var (
	_subscribe         = "subscribe"
	_unsubscribe       = "unsubscribe"
	messageBytes       = []byte("message")
	unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n")
)

func makeMsg(t string, channel string, code int64) []byte {
	return []byte("*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + reply.CRLF + t + reply.CRLF +
		"$" + strconv.FormatInt(int64(len(channel)), 10) + reply.CRLF + channel + reply.CRLF +
		":" + strconv.FormatInt(code, 10) + reply.CRLF)
}

func Subscribe(pool *SubPool, conn redis.Connection, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	for i, ch := range args {
		channels[i] = string(ch)
	}

	pool.locker.Locks(channels...)
	defer pool.locker.UnLocks(channels...)

	for _, ch := range channels {
		if subscribe0(pool, conn, ch) {
			_ = conn.Write(makeMsg(_subscribe, ch, int64(conn.SubsCount())))
		}
	}
	return &reply.NoReply{}
}

func subscribe0(subs *SubPool, conn redis.Connection, channel string) bool {
	conn.Subscribe(channel)

	raw, ok := subs.pool.Get(channel)
	var ll *list.LinkedList
	if !ok {
		ll = list.MakeLinkedList()
		subs.pool.Put(channel, ll)
	} else {
		ll, _ = raw.(*list.LinkedList)
	}

	if ll.Contain(conn) {
		return false
	}

	ll.RPush(conn)
	return true
}

func UnSubscribe(pool *SubPool, conn redis.Connection, args [][]byte) redis.Reply {
	var channels []string
	if len(args) > 0 {
		channels = make([]string, len(args))
		for i, b := range args {
			channels[i] = string(b)
		}
	} else {
		channels = conn.GetChannels()
	}

	pool.locker.Locks(channels...)
	defer pool.locker.UnLocks(channels...)

	if len(channels) == 0 {
		_ = conn.Write(unSubscribeNothing)
		return &reply.NoReply{}
	}

	pool.locker.Locks(channels...)
	defer pool.locker.UnLocks(channels...)

	for _, ch := range channels {
		if unsubscribe0(pool, conn, ch) {
			_ = conn.Write(makeMsg(_unsubscribe, ch, int64(conn.SubsCount())))
		}
	}

	return &reply.NoReply{}
}

func unsubscribe0(subs *SubPool, conn redis.Connection, channel string) bool {
	conn.UnSubscribe(channel)

	raw, ok := subs.pool.Get(channel)
	var ll *list.LinkedList
	if ok {
		ll, _ = raw.(*list.LinkedList)
		ll.RemoveByVal(conn, 1)
		if ll.Len() == 0 {
			subs.pool.Remove(channel)
		}
		return true
	}

	return false
}
