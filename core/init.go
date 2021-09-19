package core

import "Tiny-Godis/lib/config"

func Init() error {
	err := initConfig()
	if err != nil {
		return err
	}

	return nil
}

func initConfig() error {
	config.Parse()
	err := config.SetupConfig()
	return err
}
