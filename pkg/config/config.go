package config

import (
	"github.com/kelseyhightower/envconfig"
	"github.com/sirupsen/logrus"
)

// Config is used to configure the service
type Config struct {
	Port        int    `envconfig:"PORT" default:"1337"`
	LogLevel    string `envconfig:"LOG_LEVEL" default:"INFO"`
	StorageFile string `envconfig:"STORAGE_FILE" default:"/var/lib/goethe/data"`
}

// Get retrieves the config from environment
func Get() Config {
	config := Config{}

	err := envconfig.Process("goethe", &config)
	if err != nil {
		panic("config error: " + err.Error())
	}

	lvl, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		panic("config error: " + err.Error())
	}
	logrus.SetLevel(lvl)

	return config
}
