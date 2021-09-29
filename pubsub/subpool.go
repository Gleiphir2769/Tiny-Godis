package pubsub

import (
	"Tiny-Godis/data_struct/dict"
	"Tiny-Godis/data_struct/lock"
)

type SubPool struct {
	pool   dict.Dict
	locker lock.Locks
}

func MakeSubPool() *SubPool {
	return &SubPool{
		pool:   dict.MakeConcurrent(4),
		locker: lock.Locks{},
	}
}
