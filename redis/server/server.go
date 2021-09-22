package server

import (
	"Tiny-Godis/core"
	"Tiny-Godis/interface/db"
	"Tiny-Godis/lib/logger"
	"Tiny-Godis/lib/sync/atomic"
	"Tiny-Godis/redis/connection"
	"Tiny-Godis/redis/parser"
	"Tiny-Godis/redis/reply"
	"context"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type Handler struct {
	activeConn sync.Map
	db         db.DB
	closing    atomic.Boolean
}

func MakeHandler() *Handler {
	return &Handler{db: core.MakeDB()}
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
	}

	client := connection.MakeConn(conn)
	h.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}

			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

func (h *Handler) Close() error {
	return nil
}

func (h *Handler) closeClient(client *connection.Connection) {
	client.Close()
	//h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}
