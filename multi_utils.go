package Tiny_Godis

import "Tiny-Godis/lib/utils"

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
		if _, ok := db.GetEntity(key); ok {
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("DEL", key))
		} else {
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("DEL", key))
			// todo: 等待marshal模块完成
		}
	}
	return undoCmdLines
}

func rollbackFirstKey(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	return rollbackGivenKeys(db, key)
}
