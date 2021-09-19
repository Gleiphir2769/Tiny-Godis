package Tiny_Godis

import (
	"Tiny-Godis/lib/utils"
)

func readFirstKey(args [][]byte) ([]string, []string) {
	// assert len(args) > 0
	key := string(args[0])
	return nil, []string{key}
}

func writeFirstKey(args [][]byte) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func writeAllKeys(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return keys, nil
}

func readAllKeys(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return nil, keys
}

func noPrepare(args [][]byte) ([]string, []string) {
	return nil, nil
}

func rollbackGivenKeys(db *DB, keys ...string) []CmdLine {
	var undoCmdLines []CmdLine
	for _, key := range keys {
		if entity, ok := db.GetEntity(key); ok {
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("DEL", key))
		} else {
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("DEL", key),
				EntityToSetCmd(key, entity).Args,
				toTTLCmd(db, key).Args)
		}
	}
	return undoCmdLines
}

func rollbackFirstKey(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	return rollbackGivenKeys(db, key)
}

func rollbackHashFields(db *DB, key string, fields ...string) []CmdLine {
	var undoCmdLines []CmdLine
	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return nil
	}
	if d == nil {
		undoCmdLines = append(undoCmdLines, utils.ToCmdLine("DEL", key))
		return undoCmdLines
	}
	for _, field := range fields {
		if entity, ok := d.Get(field); !ok {
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("HDEL", key, field))
		} else {
			value, _ := entity.([]byte)
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("HSET", key, field, string(value)))
		}
	}
	return undoCmdLines
}

func rollbackSetMembers(db *DB, key string, members ...string) []CmdLine {
	var undoCmdLines []CmdLine
	s, errReply := db.getAsSet(key)
	if errReply != nil {
		return nil
	}
	if s == nil {
		undoCmdLines = append(undoCmdLines, utils.ToCmdLine("DEL", key))
		return undoCmdLines
	}
	for _, member := range members {
		if !s.Has(member) {
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("SREM", key, member))
		} else {
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("SADD", key, member))
		}
	}
	return undoCmdLines
}
