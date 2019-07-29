package mqwrap

import (
	"sync"

	"github.com/streadway/amqp"
)

const mqExchName = "scope.polling"

// MQWrap is struct for RabbitMQ wrap work
type MQWrap struct {
	ExchangeType    string
	ContentType     string
	ContentEncoding string
	rpcQueueName    string
	Name            string
	ExchangeName    string
	consumer        *consumer
	consumerMutex   sync.Mutex
	conn            *amqp.Connection
	channel         *amqp.Channel
	ErrorChan       chan amqp.Return
	wg              *sync.WaitGroup
	sem             *Semaphore
	Prefetch        int
}
