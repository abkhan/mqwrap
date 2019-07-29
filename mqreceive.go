package mqwrap

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type MessageDelivery struct {
	Delivery amqp.Delivery
	Message  interface{}
	Error    error
	//	Context  *Context
}

type MessageGenerator func() interface{}
type MessageHandler func(MessageDelivery) (interface{}, error)

// NewMQReceiver is called by user to get a reciever handle
func NewMQReceiver(name string) *MQWrap {

	c := GetMQConfig()
	return mqReceiver(name, c)
}

// NewMQReceiverWithConfig is called by user with RabbitMQ config info to get a reciever handle
func NewMQReceiverWithConfig(name, host, port, user, pass string) *MQWrap {
	mqconf := RMQConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Pass:     pass,
		Prefetch: 20,
	}
	return mqReceiver(name, mqconf)
}

func mqReceiver(name string, mqconf RMQConfig) *MQWrap {

	// try to connect
	connString := fmt.Sprintf("amqp://%s:%s@%s:%s", mqconf.User, mqconf.Pass, mqconf.Host, mqconf.Port)
	config := amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, 3*time.Second)
		},
		Heartbeat: 20 * time.Second,
	}

	mqr := MQWrap{}
	var err error
	if mqr.conn, err = amqp.DialConfig(connString, config); err != nil {
		log.Fatalf("Can't connect to RabbitMQ")
	}
	go waitOnConnError(mqr.conn)

	mqr.Name = name
	mqr.ExchangeName = os.Getenv("RABBIT_MQ_EXCH")
	mqr.ExchangeType = "topic"
	mqr.ContentType = "application/json"
	mqr.ContentEncoding = "gzip"
	mqr.ErrorChan = make(chan amqp.Return)
	mqr.wg = &sync.WaitGroup{}
	mqr.sem = NewSemaphore(256)
	mqr.rpcQueueName = name
	mqr.Prefetch = mqconf.Prefetch
	return &mqr
}

func waitOnConnError(c *amqp.Connection) {
	connErrChan := make(chan *amqp.Error, 1)
	c.NotifyClose(connErrChan)

	//Reconnect after a transport or protocol error
	connErr := <-connErrChan
	if connErr != nil {
		log.Errorf("Got connection error to rabbitmq: %+v", connErr)
	} else {
		//Normal shutdown
		log.Info("Got connection to rabbitmq normal shutdown event.")
	}
	os.Exit(1)
}

// AddHandler on the MQReciever adds a handle function that calls it when it receives a message
func (mq *MQWrap) AddHandler(queueName string, routingKeys []string, autoDelete bool, mg MessageGenerator, mh MessageHandler) error {
	if mq.consumer != nil {
		return errors.New("Consumer exists")
	}

	c := &consumer{
		QueueName:       queueName,
		RoutingKeys:     routingKeys,
		exclusiveQueue:  false,
		autoDeleteQueue: true,
		mqconn:          mq,
	}
	mq.consumer = c
	mdc, err := c.consume(mg)
	if err != nil {
		return err
	}

	mq.wg.Add(1)
	go func(workerChan chan MessageDelivery) {
		for md := range workerChan {
			mq.sem.Up()
			go func(sem *Semaphore, md MessageDelivery) {
				defer sem.Down()

				//correlationHeader := ""
				//if ch, ok := md.Delivery.Headers["correlationHeader"]; ok {
				//		correlationHeader, _ = ch.(string)
				//	}

				log.Debugf("Received request %+v with correlation header: %v and transactionName: %v\n\n", md.Message, md.Delivery.Headers["correlationHeader"], md.Delivery.Headers["transactionName"])
				//transactionName := queueName

				result, err := mh(md)
				log.Debugf("Handler: Ret: %+v, %+v", result, err)

				if err != nil {
					mq.ReplyError("Failed in "+md.Delivery.RoutingKey, 1, err, md.Delivery)
				} else if result != nil {
					mq.Reply(result, md.Delivery)
				}

			}(mq.sem, md)
		}
		mq.wg.Done()
	}(mdc)

	return nil
}

func (mq *MQWrap) AddRouting(n, rk string) error {
	return nil
}
func (mq *MQWrap) RemRouting(n, rk string) error {
	return nil
}

//Reply should send back a reply, but only writes the reply
func (mq *MQWrap) Reply(message interface{}, d amqp.Delivery) error {
	log.Warnf("Reply: %+v", message)
	return nil
}

//ReplyError Returns the error messages to the RPCClient if request was unsuccessful
func (mq *MQWrap) ReplyError(emess string, code int, err error, d amqp.Delivery) error {
	log.Errorf("ReplyError: %s, Err: %v", emess, err)
	return nil
}
