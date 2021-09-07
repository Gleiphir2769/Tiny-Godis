package parser

import (
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/lib/utils"
	"Tiny-Godis/redis/reply"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestParseLine(t *testing.T) {
	testString := "value\r\n"
	testBytes := []byte(testString)
	br := bytes.NewBuffer(testBytes)
	rbf := bufio.NewReader(br)
	state := readState{
		readingMultiLine:  false,
		expectedArgsCount: 0,
		msgType:           0,
		args:              nil,
		bulkLen:           5,
	}

	res, ioEOF, err := readLine(rbf, &state)
	if err != nil {
		t.Error(err)
		return
	}
	if ioEOF == true {
		t.Error("io EOF")
		return
	}
	if string(res) != "value\r\n" {
		t.Error(fmt.Sprintf("parse failed: %s", string(res)))
		return
	}
}

func TestParseStream(t *testing.T) {
	replies := []redis.Reply{
		reply.MakeIntReply(1),
		reply.MakeStatusReply("OK"),
		reply.MakeErrReply("ERR unknown"),
		reply.MakeBulkReply([]byte("a\r\nb")), // test binary safe
		reply.MakeNullBulkReply(),
		reply.MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		reply.MakeEmptyMultiBulkReply(),
	}
	reqs := bytes.Buffer{}
	for _, re := range replies {
		reqs.Write(re.ToBytes())
	}
	reqs.Write([]byte("set a a" + reply.CRLF)) // test text protocol
	expected := make([]redis.Reply, len(replies))
	copy(expected, replies)
	expected = append(expected, reply.MakeMultiBulkReply([][]byte{
		[]byte("set"), []byte("a"), []byte("a"),
	}))

	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	i := 0
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		}
		exp := expected[i]
		i++
		if !utils.BytesEquals(exp.ToBytes(), payload.Data.ToBytes()) {
			t.Error("parse failed: " + string(exp.ToBytes()))
		}
	}
}

func TestParseOne(t *testing.T) {
	replies := []redis.Reply{
		reply.MakeIntReply(1),
		reply.MakeStatusReply("OK"),
		reply.MakeErrReply("ERR unknown"),
		reply.MakeBulkReply([]byte("a\r\nb")), // test binary safe
		reply.MakeNullBulkReply(),
		reply.MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		reply.MakeEmptyMultiBulkReply(),
	}
	for _, re := range replies {
		result, err := ParseOne(re.ToBytes())
		if err != nil {
			t.Error(err)
			continue
		}
		if !utils.BytesEquals(result.ToBytes(), re.ToBytes()) {
			t.Error("parse failed: " + string(re.ToBytes()))
		}
	}
}
