package connection

import (
	"Tiny-Godis/lib/sync/atomic"
	"Tiny-Godis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// Connection represents a connection with a redis-cli
type Connection struct {
	conn net.Conn

	// waiting until reply finished
	waitingReply wait.Wait

	// lock while server sending response
	mu sync.Mutex

	// password may be changed by CONFIG command during runtime, so store the password
	password string

	// multi related
	multiState    atomic.Boolean
	watchingQueue map[string]uint32
	queue         [][][]byte
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close disconnect with the client
func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// MakeConn creates Connection instance
func MakeConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
		//watchingQueue: make(map[string]uint32),
	}
}

// Write sends response to client over tcp connection
func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	c.mu.Lock()
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(b)
	return err
}

func (c *Connection) SetPassword(pw string) {
	if len(pw) == 0 {
		return
	}

	c.password = pw
}

func (c *Connection) GetPassword() string {
	return c.password
}

func (c *Connection) InMultiState() bool {
	return c.multiState.Get()
}

func (c *Connection) SetMultiState(state bool) {
	c.multiState.Set(state)
}

func (c *Connection) GetQueuedCmdLine() [][][]byte {
	if c.queue == nil {
		c.queue = make([][][]byte, 0)
	}
	return c.queue
}

func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	if c.queue == nil {
		c.queue = make([][][]byte, 0)
	}
	c.queue = append(c.queue, cmdLine)
}

func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

func (c *Connection) GetWatching() map[string]uint32 {
	if c.watchingQueue == nil {
		c.watchingQueue = make(map[string]uint32)
	}
	return c.watchingQueue
}
