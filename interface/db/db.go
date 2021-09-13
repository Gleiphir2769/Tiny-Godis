package db

import "Tiny-Godis/interface/redis"

// DB is the interface for redis style storage engine
type DB interface {
	Exec(client redis.Connection, args [][]byte) redis.Reply
	AfterClientClose(c redis.Connection)
	Close()
}
