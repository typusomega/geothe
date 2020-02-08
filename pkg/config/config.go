package config

import (
	"github.com/kelseyhightower/envconfig"
	"github.com/sirupsen/logrus"
)

// Config is used to configure the service.
type Config struct {
	APIPort        int    `envconfig:"API_PORT" default:"1337"`
	PrometheusPort int    `envconfig:"PROMETHEUS_PORT" default:"5000"`
	LogLevel       string `envconfig:"LOG_LEVEL" default:"INFO"`
	DataDir        string `envconfig:"DATA_DIR" default:"/var/lib/goethe/data"`
	Raft           Raft   `envconfig:"RAFT"`
}

// Raft bundles raft specific config.
type Raft struct {
	NodeName      string `envconfig:"NODE_NAME" default:"goethe-0"`
	Port          int    `envconfig:"PORT" default:"1338"`
	LogRetention  int    `envconfig:"LOG_RETENTION" default:"10000"`
	Bootstrap     bool   `envconfig:"BOOTSTRAP" default:"true"`
	BootstrapNode string `envconfig:"BOOTSTRAP_NODE" default:""`
}

// Get retrieves the config from environment.
func Get() Config {
	config := Config{}

	err := envconfig.Process("", &config)
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
