package Tiny_Godis

import (
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/redis/reply"
)

// todo: 等待config模块完成
const requirePass = "112233"

func Ping(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &reply.PongReply{}
	} else if len(args) == 1 {
		return reply.MakeStatusReply(string(args[0]))
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

func Auth(conn redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}
	userPass := string(args[0])
	if requirePass == "" {
		return reply.MakeErrReply("ERR Client sent AUTH, but no password is set")
	}
	conn.SetPassword(userPass)
	if requirePass != userPass {
		return reply.MakeErrReply("ERR Invalid Password")
	} else {
		return &reply.OkReply{}
	}
}

func init() {
	RegisterCommand("ping", Ping, noPrepare, nil, -1)
}
