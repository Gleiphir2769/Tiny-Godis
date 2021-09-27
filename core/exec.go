package core

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
		return Auth(conn, cmdLine)
	}

	r, done := db.execSpecialCmd(conn, cmdLine)
	if done {
		return r
	}

	if conn != nil && conn.InMultiState() {
		return EnqueueCmd(conn, cmdLine)
	}

	return db.execNormalCmd(cmdLine)
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
	case "bgrewriteaof":
		// aof.go imports cmd.go, cmd.go cannot import BGRewriteAOF from aof.go
		return BGRewriteAOF(db, cmdLine[1:]), true
	case "multi":
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName), true
		}
		return StartMulti(conn), true
	case "discard":
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName), true
		}
		return DiscardMulti(conn), true
	case "exec":
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName), true
		}
		return ExecMulti(db, conn), true
	case "watch":
		if !validateArity(-2, cmdLine) {
			return reply.MakeArgNumErrReply(cmdName), true
		}
		return Watch(db, conn, cmdLine[1:]), true
	default:
		return nil, false
	}
}

func (db *DB) execNormalCmd(cmdLine CmdLine) (result redis.Reply) {
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

func (db *DB) ExecWithLock(cmdLine CmdLine) (result redis.Reply) {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}

	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	return cmd.executor(db, cmdLine[1:])
}

func (db *DB) GetRelatedKey(cmdLine CmdLine) (writeKey []string, readKey []string) {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil, nil
	}
	// todo: 是否需要验证参数数量
	if !validateArity(cmd.arity, cmdLine) {
		return nil, nil
	}
	if cmd.prepare == nil {
		return nil, nil
	}
	return cmd.prepare(cmdLine[1:])
}

func (db *DB) GetUndoLog(cmdLine CmdLine) (undoLog []CmdLine) {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil
	}
	// todo: 是否需要验证参数数量
	if !validateArity(cmd.arity, cmdLine) {
		return nil
	}
	if cmd.undo == nil {
		return nil
	}
	return cmd.undo(db, cmdLine[1:])
}
