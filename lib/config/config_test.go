package config

import (
	"fmt"
	"testing"
)

func TestConfig(t *testing.T) {
	Parse()
	err := SetupConfig()
	if err != nil {
		t.Error(err)
	}
	fmt.Println("bind: ", Properties.Bind)
	fmt.Println("port: ", Properties.Port)
	fmt.Println("maxClient: ", Properties.MaxClients)
	fmt.Println("appendonly: ", Properties.AppendOnly)
	fmt.Println("AppendFilename: ", Properties.AppendFilename)
	fmt.Println("RequirePass: ", Properties.RequirePass)
	fmt.Println("Peers: ", Properties.Peers)
	fmt.Println("Self: ", Properties.Self)
}
