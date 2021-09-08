package parser

import (
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/lib/logger"
	"Tiny-Godis/redis/reply"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// Payload stores redis.Reply or error
type Payload struct {
	Data redis.Reply
	Err  error
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	result := make([]redis.Reply, 0)
	reader := bytes.NewBuffer(data)
	go parse0(reader, ch)
	for payload := range ch {
		if payload == nil {
			return nil, fmt.Errorf("no reply")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		result = append(result, payload.Data)
	}
	return result, nil
}

func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewBuffer(data)
	go parse0(reader, ch)
	payload := <-ch
	if payload == nil {
		return nil, fmt.Errorf("no reply")
	}
	return payload.Data, payload.Err
}

// 解析器是一个依托于state的有限状态机，通过每次解析对state进行操作进行跳转，返回不同的reply
func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	var result redis.Reply
	for {
		var ioEOF bool
		msg, ioEOF, err = readLine(bufReader, &state)
		if err != nil {
			if ioEOF {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}

			ch <- &Payload{
				Err: err,
			}
			close(ch)
			continue
		}
		if !state.readingMultiLine {
			if msg[0] == '*' {
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{Err: err}
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{Err: err}
					state = readState{}
					continue
				}
				if state.bulkLen == -1 { // null bulk reply
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else {
				result, err = parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else {
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{Err: err}
				state = readState{}
				continue
			}
			if state.finished() {
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{Data: result}
				state = readState{}
			}
		}
	}
}

// ioEOF 的作用是将协议解析错误与其他致命错误区别开，供解析器识别
func readLine(reader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var err error
	var msg []byte

	if state.bulkLen == 0 {
		msg, err = reader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, fmt.Errorf("protocal error: %s", string(msg))
		}
	} else {
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(reader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, fmt.Errorf("protocal error: %s", string(msg))
		}
		state.bulkLen = 0
	}

	return msg, false, nil
}

func readBody(msg []byte, state *readState) error {
	line := msg[:len(msg)-2]
	var err error
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return fmt.Errorf("protocal error: %s", string(msg))
		}
		if state.bulkLen <= 0 {
			// todo: 重复添加空字符串????
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}

func parseMultiBulkHeader(msg []byte, state *readState) error {
	line := msg[1 : len(msg)-2]
	expectedLine, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return fmt.Errorf("protocal error: %s", string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return fmt.Errorf("protocal error: %s", string(msg))
	}
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	line := msg[1 : len(msg)-2]
	state.bulkLen, err = strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return fmt.Errorf("protocal error: %s", string(msg))
	}
	if state.bulkLen == -1 {
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
	} else {
		return fmt.Errorf("protocal error: %s", string(msg))
	}
	return nil
}

func parseSingleLineReply(msg []byte) (redis.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result redis.Reply
	switch msg[0] {
	case '+':
		result = reply.MakeStatusReply(str[1:])
	case '-':
		result = reply.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("protocal error: %s", string(msg))
		}
		result = reply.MakeIntReply(val)
	// todo: redis通信协议并没有text protocol
	default:
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for index, s := range strs {
			args[index] = []byte(s)
		}
		result = reply.MakeMultiBulkReply(args)
	}

	return result, nil
}
