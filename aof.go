package Tiny_Godis

import (
	"Tiny-Godis/lib/config"
	"Tiny-Godis/lib/logger"
	"Tiny-Godis/redis/parser"
	"Tiny-Godis/redis/reply"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var pExpireAtBytes = []byte("PEXPIREAT")

func makeAofCmd(cmd string, args [][]byte) *reply.MultiBulkReply {
	params := make([][]byte, len(args)+1)
	params[0] = []byte(cmd)
	copy(params[1:], args)
	return reply.MakeMultiBulkReply(params)
}

func makeExpireAofCmd(key string, expireAt time.Time) *reply.MultiBulkReply {
	params := make([][]byte, 3)
	params[0] = pExpireAtBytes
	params[1] = []byte(key)
	params[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return reply.MakeMultiBulkReply(params)
}

func (db *DB) AddAof(args *reply.MultiBulkReply) {
	if config.Properties.AppendOnly && db.aofChan != nil {
		db.aofChan <- args
	}
}

func (db *DB) handleAof() {
	for cmd := range db.aofChan {
		db.aofPause.RLock()
		func() {
			defer db.aofPause.RUnlock()
			if db.aofRewriteBuffer != nil {
				db.aofRewriteBuffer <- cmd
			}
			_, err := db.aofFile.Write(cmd.ToBytes())
			if err != nil {
				logger.Warn(err)
			}
		}()
	}
	db.aofFinished <- struct{}{}
}

func (db *DB) loadAof(maxByte int64) {
	aofChan := db.aofChan
	db.aofChan = nil
	defer func() {
		db.aofChan = aofChan
	}()

	f, err := os.Open(db.aofFileName)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer func() {
		_ = f.Close()
	}()

	var reader io.Reader
	if maxByte > 0 {
		reader = io.LimitReader(f, maxByte)
	} else {
		reader = f
	}

	ch := parser.ParseStream(reader)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				// connection closed
				break
			}
			logger.Error("parse error: " + payload.Err.Error())
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		args, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		cmd := strings.ToLower(string(args.Args[0]))
		command, ok := cmdTable[cmd]
		if ok {
			command.executor(db, args.Args[1:])
		}
	}
}
