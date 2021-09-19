package config

import (
	"sync"

	"github.com/spf13/viper"
)

var (
	Properties *ServerProperties
	onceConfig sync.Once
)

func init() {
	// default config
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
	}
}

// ServerProperties defines global config properties
type ServerProperties struct {
	Bind           string `yaml:"bind"`
	Port           int    `yaml:"port"`
	AppendOnly     bool   `yaml:"appendOnly"`
	AppendFilename string `yaml:"appendFilename"`
	MaxClients     int    `yaml:"maxclients"`
	RequirePass    string `yaml:"requirepass"`

	Peers []string `yaml:"peers"`
	Self  string   `yaml:"self"`
}

// SetupConfig read config file and store properties into Properties
func SetupConfig() error {
	err := initViper()
	if err != nil {
		return err
	}
	onceConfig.Do(func() {
		Properties = &ServerProperties{
			Bind:           viper.GetString("bind"),
			Port:           viper.GetInt("port"),
			AppendOnly:     viper.GetBool("appendOnly"),
			AppendFilename: viper.GetString("appendFilename"),
			MaxClients:     viper.GetInt("maxclients"),
			RequirePass:    viper.GetString("requirepass"),

			Peers: viper.GetStringSlice("peers"),
			Self:  viper.GetString("self"),
		}
	})
	return nil
}
