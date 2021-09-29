package client

import (
	"Tiny-Godis/interface/redis"
	"Tiny-Godis/lib/logger"
	"Tiny-Godis/lib/sync/wait"
	"Tiny-Godis/redis/parser"
	"Tiny-Godis/redis/reply"
	"net"
	"time"
)

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

type Client struct {
	conn        net.Conn
	sendingChan chan *request
	waitingChan chan *request
	waiting     *wait.Wait
	addr        string
	ticker      *time.Ticker
}

type request struct {
	id      uint64
	args    [][]byte
	reply   redis.Reply
	waiting *wait.Wait
	err     error
}

func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:        conn,
		sendingChan: make(chan *request, chanSize),
		waitingChan: make(chan *request, chanSize),
		waiting:     &wait.Wait{},
		addr:        addr,
	}, nil
}

func (c *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	cmdline := reply.MakeMultiBulkReply(req.args)
	_, err := c.conn.Write(cmdline.ToBytes())
	i := 0
	for err != nil && i < 3 {
		err = c.redialIfFailed()
		if err != nil {
			logger.Error(err)
		} else {
			_, err = c.conn.Write(cmdline.ToBytes())
		}
		i++
	}
	if err == nil {
		c.waitingChan <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (c *Client) redialIfFailed() error {
	err := c.conn.Close()
	if err != nil {
		if opErr, ok := err.(*net.OpError); ok {
			if opErr.Err.Error() != "use of closed network connection" {
				return err
			}
		} else {
			return err
		}
	}
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	c.conn = conn
	go func() {
		// 重启监视读隧道进程
	}()
	return nil
}

func (c *Client) handleRead() {
	ch := parser.ParseStream(c.conn)
	for payload := range ch {
		if payload.Err != nil {
			c.receive(reply.MakeErrReply(payload.Err.Error()))
			continue
		}
		c.receive(payload.Data)
	}
}

func (c *Client) receive(rp redis.Reply) {
	req := <-c.waitingChan
	if req == nil {
		return
	}
	req.reply = rp
	if req.waiting != nil {
		req.waiting.Done()
	}
}

func (c *Client) handleWrite() {
	for req := range c.sendingChan {
		c.doRequest(req)
	}
}

func (c *Client) doHeartbeat() {
	args := [][]byte{[]byte("PING")}
	_ = c.Send(args)
}

func (c Client) Send(args [][]byte) redis.Reply {
	return c.send(args)
}

func (c *Client) send(args [][]byte) redis.Reply {
	req := &request{
		id:      uint64(time.Now().UnixNano()),
		args:    args,
		waiting: &wait.Wait{},
	}
	req.waiting.Add(1)
	c.waiting.Add(1)
	defer c.waiting.Done()
	c.sendingChan <- req
	req.waiting.WaitWithTimeout(maxWait)
	if req.err != nil {
		return reply.MakeErrReply("request failed: " + req.err.Error())
	}
	return req.reply
}

func (c *Client) heartbeat() {
	for range c.ticker.C {
		c.doHeartbeat()
	}
}

func (c *Client) Start() {
	c.ticker = time.NewTicker(10 * time.Second)
	go c.handleWrite()
	go c.handleRead()
	go c.heartbeat()
}

func (c *Client) Close() {
	c.ticker.Stop()

	close(c.sendingChan)

	c.waiting.Wait()

	_ = c.conn.Close()
	close(c.waitingChan)
}
