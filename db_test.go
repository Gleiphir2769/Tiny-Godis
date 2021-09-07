package Tiny_Godis

import (
	"fmt"
	"testing"
)

func TestDBFeature(t *testing.T) {
	db := MakeDB()
	te := DataEntity{Data: "123123"}
	n := db.PutEntity("test", &te)
	fmt.Println(n)

}
