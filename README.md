# mqwrap
Library to wrap Rabbit-MQ communication as communication service

# Current Limitations
The library currently only allows reading from a topic exchange

# Use
First Create a receiver using; NewMQReader(), then add a handler using AddHandler()

#Example
```
	// A service to receive polling updates
	pollingService = mqwrap.NewMQReceiver(listen_queue_name)
	pollingService.ExchangeName = conf.App.ExchangeName
	pollingService.ExchangeType = conf.App.ExchangeType

	// Also create the receve Q, without RK
	if e := pollingService.AddHandler(listen_queue_name, []string{listen_rk_name}, false, retStatus, fixedHandler); e != nil {
		log.Fatalf("!!! Q[%s] creation failed. Big problem.", conf.App.Clear.Queue)
	}
```


Where;
```
func retStatus() interface{} {
	return &StatusMessage{}
}
```

and the signature of fixedHandler is;
```
func fixedHandler(md mqwrap.MessageDelivery) (interface{}, error) 
```

# Setup
Provide RabbitMQ connection info in Env Vars;

RABBIT_MQ_HOST

RABBIT_MQ_PORT

RABBIT_MQ_USER

RABBIT_MQ_PASSW
