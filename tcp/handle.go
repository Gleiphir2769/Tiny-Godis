package tcp

import (
	"bufio"
	"context"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

type Client struct {
	Conn net.Conn

	Waiting wait.Wait
}

type EchoHandler struct {
	activeConn sync.Map

	closing atomic.Boolean
}

func NewEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func (eh *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if eh.closing.Get() {
		conn.Close()
	}

	client := &Client{Conn: conn}
	eh.activeConn.Store(client, struct{}{})

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				eh.activeConn.Delete(conn)
			} else {
				logger.Warn(err)
			}
			return
		}
		// 发送数据前先置为waiting状态，阻止连接被关闭(client wait 置状态，等待client所有数据传输完)
		client.Waiting.Add(1)

		// 模拟关闭时未完成发送的情况
		//logger.Info("sleeping")
		//time.Sleep(10 * time.Second)

		b := []byte(msg)
		conn.Write(b)
		// 发送完毕, 结束waiting
		client.Waiting.Done()
	}
}

func (c *Client) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second)
	c.Conn.Close()
	return nil
}

func (eh *EchoHandler) Close() error {
	logger.Info("handler shutting down...")
	eh.closing.Set(true)

	eh.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*Client)
		client.Close()
		return true
	})
	return nil
}
