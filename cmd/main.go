package main

import (
	"Tiny-Godis/core"
	"Tiny-Godis/lib/config"
	"Tiny-Godis/lib/logger"
	"Tiny-Godis/redis/server"
	"Tiny-Godis/tcp"
	"fmt"
)

var banner = `
 _____  _                ____           _  _      
|_   _|(_)_ __  _   _   / ___| ___   __| |(_) ___ 
  | |  | | '_ \| | | | | |  _ / _ \ / _  || |/ __|
  | |  | | | | | |_| | | |_| | (_) | (_| || |\__ \
  |_|  |_|_| |_|\__, |  \____|\___/ \__,_||_||___/
                |___/
`

func main() {
	print(banner)
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	err := core.Init()
	if err != nil {
		logger.Fatal(err)
		panic(err)
	}
	cfg := tcp.Config{
		Address:    fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
		MaxConnect: uint32(config.Properties.MaxClients),
	}
	sh := server.MakeHandler()
	err = tcp.ListenAndServeWithSignal(&cfg, sh)
	if err != nil {
		logger.Fatal(err)
		panic(err)
	}
}
