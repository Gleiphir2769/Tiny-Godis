package main

import (
	"Tiny-Godis/lib/logger"
	"Tiny-Godis/tcp"
	"time"
)

func main() {
	config := tcp.Config{
		Address:    "127.0.0.1:9090",
		MaxConnect: 10,
		Timeout:    time.Second * 10,
	}
	sh := tcp.EchoHandler{}
	err := tcp.ListenAndServeWithSignal(&config, &sh)
	if err != nil {
		logger.Fatal(err)
	}

}
