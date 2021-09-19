package config

import (
	"github.com/spf13/pflag"
)

var (
	cfg     = pflag.StringP("ConfigData", "c", "", "qbus-manager ConfigData file path")
	version = pflag.BoolP("version", "v", false, "show version info.")
	Flag    *flags
)

type flags struct {
	Cfg     string
	Version bool
}

func NewFlags(cfg string, version bool) *flags {
	return &flags{Cfg: cfg, Version: version}
}

func Parse() {
	pflag.Parse()
	Flag = NewFlags(*cfg, *version)
}
