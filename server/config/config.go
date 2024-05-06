package config

import (
	"log"
	"os"
	"sync"

	"github.com/go-yaml/yaml"
)

type Config struct {
	Port int
}

var (
	once sync.Once
	cfg  Config
)

func LoadConfig(path string) {

	once.Do(func() {
		bytes, err := os.ReadFile(path)
		if err != nil {
			log.Fatalf("err reading config : %v", err)
		}

		err = yaml.Unmarshal(bytes, &cfg)
		if err != nil {
			log.Fatalf("err reading yaml config file : %v", err)
		}

	})
}

func GetConfig() Config {
	LoadConfig("./config.yaml")
	return cfg
}
