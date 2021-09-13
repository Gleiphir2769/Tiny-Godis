package Tiny_Godis

import (
	"Tiny-Godis/redis/connection"
	"fmt"
	"testing"
	"time"
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

func TestDB_Expire(t *testing.T) {
	db := MakeDB()
	te := DataEntity{}
	te.Data = "123123"
	r := db.PutEntity("test", &te)
	if r != 1 {
		fmt.Println(r)
		t.Error("db.Put failed")
		return
	}
	db.Expire("test", time.Now().Add(time.Second))
	time.Sleep(time.Second * 2)
	if _, ok := db.GetEntity("test"); ok {
		t.Error("db.Expire failed")
		return
	}
}

func TestDB_Exec(t *testing.T) {
	db := MakeDB()
	conn := connection.MakeConn(nil)
	cmdLine := make([][]byte, 0)
	cls := []string{"SET", "testKey", "testValue"}
	for _, cl := range cls {
		cmdLine = append(cmdLine, []byte(cl))
	}
	rp := db.Exec(conn, cmdLine)
	if !(string(rp.ToBytes()) == "+OK\r\n") {
		fmt.Println(string(rp.ToBytes()))
		t.Error("db.exec set failed")
	}
	ncls := []string{"GET", "testKey"}
	cmdLine2Get := make([][]byte, 0)
	for _, cl := range ncls {
		cmdLine2Get = append(cmdLine2Get, []byte(cl))
	}
	rp = db.Exec(conn, cmdLine2Get)
	validresult := []byte("$" + "9" + "\r\n" + "testValue" + "\r\n")
	if !(string(rp.ToBytes()) == string(validresult)) {
		fmt.Println(string(rp.ToBytes()))
		t.Error("db.exec get failed")
	}
}
