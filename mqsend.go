package mqwrap

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// NewMQSender is called by user to get a reciever handle
func NewMQSender(name string) *MQWrap {

	c := GetMQConfig()
	return rabbitConnect("Sender", c)
}

// SendToRabbit handles the message send to rabbit as well as the retry logic with backoff
func (mq *MQWrap) SendToRabbit(msg interface{}, rk, xch, retRK string) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	pub := amqp.Publishing{Body: body}
	if retRK != "" {
		pub.ReplyTo = retRK
	}
	if mq != nil {
		if mq.channel != nil {
			log.Infof("SendToRabbit: Body: %s", string(body))
			return mq.channel.Publish(xch, rk, false, false, pub)
		} else {
			log.Error("!!! mq.channel is nil")
		}
	}

	log.Errorf("mq:%T is nil", mq)
	return errors.New("mq or channel is nil")
}

// rabbitConnect does a rabiit connection using config passed in
func rabbitConnect(name string, mqconf RMQConfig) *MQWrap {

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

	mqr.channel, err = mqr.conn.Channel()
	if err != nil {
		log.Fatalf("Can't connect to RabbitMQ")
	}

	if err = mqr.channel.ExchangeDeclare(
		mqExchName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil
	}

	return &mqr
}
