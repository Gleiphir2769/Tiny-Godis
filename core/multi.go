package core

import (
	"Tiny-Godis/data_struct/set"
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/redis/reply"
)

var forbiddenCmdInMulti = set.MakeSet("flushdb", "flushall")

func Watch(db *DB, conn redis.Connection, args [][]byte) redis.Reply {
	watching := conn.GetWatching()
	for _, key := range args {
		watching[string(key)] = db.getVersion(string(key))
	}
	return reply.MakeOkReply()
}

func execGetVersion(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	ver := db.getVersion(key)
	return reply.MakeIntReply(int64(ver))
}

func init() {
	RegisterCommand("GetVer", execGetVersion, readAllKeys, nil, 2)
}

func isWatchChanged(db *DB, watching map[string]uint32) bool {
	for watchKey, ver := range watching {
		if db.getVersion(watchKey) != ver {
			return true
		}
	}
	return false
}

func StartMulti(conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return reply.MakeErrReply("ERR MULTI calls can not be nested")
	}
	conn.SetMultiState(true)
	return reply.MakeOkReply()
}

func EnqueueCmd(conn redis.Connection, line CmdLine) redis.Reply {
	cmdName := string(line[0])
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if forbiddenCmdInMulti.Has(cmdName) {
		return reply.MakeErrReply("ERR command '" + cmdName + "' can not used in MULTI")
	}
	if cmd.prepare == nil {
		return reply.MakeErrReply("ERR command '" + cmdName + "' can not used in MULTI")
	}
	if !validateArity(cmd.arity, line) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	conn.EnqueueCmd(line)
	return reply.MakeQueuedReply()
}

func ExecMulti(db *DB, conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return reply.MakeErrReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	cmdLines := conn.GetQueuedCmdLine()
	return execMulti(db, conn.GetWatching(), cmdLines)
}

func execMulti(db *DB, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	writeKeys := make([]string, 0)
	readKeys := make([]string, 0)
	for _, cmdline := range cmdLines {
		cmdName := string(cmdline[0])
		cmd, _ := cmdTable[cmdName]
		wk, rk := cmd.prepare(cmdline[1:])
		writeKeys = append(writeKeys, wk...)
		readKeys = append(readKeys, rk...)
	}

	watchingKeys := make([]string, len(watching))
	index := 0
	for key := range watching {
		watchingKeys[index] = key
		index++
	}

	readKeys = append(readKeys, watchingKeys...)

	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)

	if isWatchChanged(db, watching) {
		return reply.MakeEmptyMultiBulkReply()
	}

	results := make([]redis.Reply, len(cmdLines))
	undoLogs := make([][]CmdLine, len(cmdLines))
	aborted := false
	for i, cmdline := range cmdLines {
		undoLogs[i] = db.GetUndoLog(cmdline)
		rp := db.ExecWithLock(cmdline)
		if reply.IsErrorReply(rp) {
			aborted = true
			undoLogs = undoLogs[:i]
			break
		}
		results[i] = rp
	}
	if !aborted {
		db.addVersion(writeKeys...)
		return reply.MakeMultiRawReply(results)
	}

	for i := len(undoLogs) - 1; i >= 0; i-- {
		curCmdLines := undoLogs[i]
		if len(curCmdLines) == 0 {
			continue
		}
		for _, undoCmdLine := range curCmdLines {
			db.ExecWithLock(undoCmdLine)
		}
	}

	return reply.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
}

func DiscardMulti(conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return reply.MakeErrReply("ERR DISCARD without MULTI")
	}
	conn.SetMultiState(false)
	conn.ClearQueuedCmds()
	return reply.MakeOkReply()
}
