package Tiny_Godis

import (
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/lib/logger"
	"Tiny-Godis/redis/reply"
	"fmt"
	"runtime/debug"
	"strings"
)

func (db *DB) Exec(conn redis.Connection, cmdLine CmdLine) (result redis.Reply) {
	// 这里是一个命名返回值和defer异常处理结合的trick，正常情况下defer 的异常处理函数是无法影响大函数的返回的
	// 但是通过命名返回值就可以利用闭包改变大函数返回值的值
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))

	if cmdName == "auth" {
		// todo: 做auth相关操作
	}

	r, done := db.execSpecialCmd(conn, cmdLine)
	if done {
		return r
	}

	// todo: multi相关

	return db.execNormalCmd(conn, cmdLine)
}

func (db *DB) execSpecialCmd(conn redis.Connection, cmdLine CmdLine) (result redis.Reply, done bool) {
	cmdName := strings.ToLower(string(cmdLine[0]))

	switch cmdName {
	//case "subscribe":
	//	if len(cmdLine) < 2 {
	//		return reply.MakeArgNumErrReply("subscribe"), true
	//	}
	//	return pubsub.Subscribe(db.hub, c, cmdLine[1:]), true
	//case "publish":
	//	return pubsub.Publish(db.hub, cmdLine[1:]), true
	//case "unsubscribe":
	//	return pubsub.UnSubscribe(db.hub, c, cmdLine[1:]), true
	//case "bgrewriteaof":
	//	// aof.go imports router.go, router.go cannot import BGRewriteAOF from aof.go
	//	return BGRewriteAOF(db, cmdLine[1:]), true
	//case "multi":
	//	if len(cmdLine) != 1 {
	//		return reply.MakeArgNumErrReply(cmdName), true
	//	}
	//	return StartMulti(db, c), true
	//case "discard":
	//	if len(cmdLine) != 1 {
	//		return reply.MakeArgNumErrReply(cmdName), true
	//	}
	//	return DiscardMulti(db, c), true
	//case "exec":
	//	if len(cmdLine) != 1 {
	//		return reply.MakeArgNumErrReply(cmdName), true
	//	}
	//	return execMulti(db, c), true
	//case "watch":
	//	if !validateArity(-2, cmdLine) {
	//		return reply.MakeArgNumErrReply(cmdName), true
	//	}
	//	return Watch(db, c, cmdLine[1:]), true
	default:
		return nil, false
	}
}

func (db *DB) execNormalCmd(conn redis.Connection, cmdLine CmdLine) (result redis.Reply) {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}

	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	wk, rk := cmd.prepare(cmdLine[1:])
	db.addVersion(wk...)
	db.RWLocks(wk, rk)
	defer db.RWUnLocks(wk, rk)

	return cmd.executor(db, cmdLine[1:])
}
