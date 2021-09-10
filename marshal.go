package Tiny_Godis

import "Tiny-Godis/redis/reply"

func EntityToSetCmd(key string, entity *DataEntity) *reply.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *reply.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToSetCmd(key, val)
		return cmd
	default:
		return cmd
	}
}

var setCmd = []byte("SET")

func stringToSetCmd(key string, data []byte) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = data
	return reply.MakeMultiBulkReply(args)
}
