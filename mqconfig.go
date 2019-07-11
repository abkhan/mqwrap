package mqwrap

// RMQConfig struct has configuration to connect to Rabbit MQ
type RMQConfig struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Pass     string `mapstructure:"pass"`
	Prefetch int    `mapstructure:"prefetch"`
}

func GetMQConfig() RMQConfig {
	return RMQConfig{
		Host:     os.Getenv("RABBIT_MQ_HOST"),
		Port:     os.Getenv("RABBIT_MQ_PORT"),
		User:     os.Getenv("RABBIT_MQ_USER"),
		Pass:     os.Getenv("RABBIT_MQ_PASSW"),
		Prefetch: 20,
	}
}
