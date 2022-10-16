package main

import (
	"flag"
	"log"
	"server/internal/app"

	"github.com/BurntSushi/toml"
)

var (
	configPath string
)

func init() {
	flag.StringVar(&configPath, "config-path", "configs/server.toml", "path to config file")
}

func main() {
	flag.Parse()

	config := app.NewConfig()
	_, err := toml.DecodeFile(configPath, config)
	if err != nil {
		log.Fatal(err)
	}

	s := app.New(config)
	if err := s.Start(); err != nil {
		log.Fatal(err)
	}
}
