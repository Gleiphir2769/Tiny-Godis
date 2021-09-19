package config

import "github.com/spf13/viper"

func initViper() error {
	if Flag.Cfg != "" {
		viper.SetConfigFile(Flag.Cfg)
	} else {
		viper.AddConfigPath(".")
		viper.SetConfigName("config")
	}
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	return nil
}
