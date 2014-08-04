package config

import (
	// stdlib
	"io/ioutil"

	// 3rd party
	"gopkg.in/yaml.v1"
)

type Config struct {
	Datadog_host   string `yaml:"datadog"`
	Api_key        string `yaml:"api_key"`
	Flush_interval int64  `yaml:"flush_interval"`

	Listening_port string `yaml:"udp_port"`
}

func Get_config(path string) Config {

	// Config file reading
	if path == "" {
		path = "/etc/dd-agent/datadog.conf"
	}
	config_file, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	configuration := Config{}
	err = yaml.Unmarshal(config_file, &configuration)

	// Specification for default value
	if configuration.Datadog_host == "" {
		configuration.Datadog_host = "https://app.datadoghq.com"
	}
	if configuration.Listening_port == "" {
		configuration.Listening_port = "8125"
	}
	if configuration.Flush_interval == 0 {
		configuration.Flush_interval = 15
	}

	return configuration
}
