package Tiny_Godis

import (
	"fmt"
	"testing"
)

func TestDBFeature(t *testing.T) {
	db := MakeDB()
	te := DataEntity{}
	te.Data = "123123"
	r := db.PutEntity("test", &te)
	if r != 1 {
		fmt.Println(r)
		t.Error("db.PutEntity failed")
		return
	}
	te2 := DataEntity{Data: "213"}
	r = db.PutIfExists("test", &te2)
	if r != 1 {
		fmt.Println(r)
		t.Error("db.PutIfExists failed")
		return
	}
	r = db.PutIfAbsent("test", &te2)
	if r == 1 {
		fmt.Println(r)
		t.Error("db.PutIfAbsent failed")
		return
	}
	v, ok := db.GetEntity("test")
	if !ok {
		t.Error("db.GetEntity failed")
		return
	}
	raw := *v
	rv := raw.Data.(string)
	if rv != "213" {
		t.Error("db.GetEntity failed")
		return
	}
	r = db.Removes("test")
	if _, ok = db.GetEntity("test"); ok || r != 1 {
		t.Error("db.Removes or db.Remove failed")
		return
	}
}
