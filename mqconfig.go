package mqwrap

import "os"

var mqConfig RMQConfig

// RMQConfig struct has configuration to connect to Rabbit MQ
type RMQConfig struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Pass     string `mapstructure:"pass"`
	Prefetch int    `mapstructure:"prefetch"`
	setDone  bool
}

//GetMQConfig is to return config values
func GetMQConfig() RMQConfig {
	if mqConfig.setDone {
		return mqConfig
	}

	return RMQConfig{
		Host:     os.Getenv("RABBIT_MQ_HOST"),
		Port:     os.Getenv("RABBIT_MQ_PORT"),
		User:     os.Getenv("RABBIT_MQ_USER"),
		Pass:     os.Getenv("RABBIT_MQ_PASSW"),
		Prefetch: 20,
	}
}

//SetMQConfig is to set config into a local config var
func SetMQConfig(c RMQConfig) {
	mqConfig = c
	mqConfig.setDone = true
}
