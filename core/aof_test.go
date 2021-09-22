package core

import (
	"Tiny-Godis/lib/utils"
	"testing"
)

func TestAof(t *testing.T) {
	testAofDb := makeTestAofDB()
	key := "shenjiaqi"
	value := utils.RandString(10)
	for i := 0; i < 1000; i++ {
		args := make([][]byte, 2)
		args[0] = []byte(key)
		args[1] = []byte(value)
		testAofDb.AddAof(makeAofCmd("SET", args))
	}
}
func makeTestAofDB() *DB {
	return MakeDB()
}
