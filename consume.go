package mqwrap

import (
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type consumer struct {
	RoutingKeys     []string
	QueueName       string
	mqconn          *MQReceiver
	exclusiveQueue  bool
	autoDeleteQueue bool
}

func (c *consumer) consume(mg MessageGenerator) (chan MessageDelivery, error) {
	messageChan := make(chan MessageDelivery, 10)
	go func() {
		for {
			logrus.Infof("Starting initial consume loop for %s queue", c.QueueName)
			logrus.Infof("Creating channel for %s queue consumer", c.QueueName)

			var channel *amqp.Channel
			var err error
			if c.mqconn.conn != nil {
				channel, err = c.mqconn.conn.Channel()
			} else {
				logrus.Error("Creating channel error: connection is nil")
				time.Sleep(1 * time.Second)
				continue
			}
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"error": err,
				}).Error("Failed to get channel to for consumer")
				<-time.After(1 * time.Second)
				continue
			}
			defer channel.Close()

			//Listen for channel exception
			chanError := make(chan *amqp.Error, 1)
			channel.NotifyClose(chanError)

			err = channel.ExchangeDeclare(c.mqconn.ExchangeName, c.mqconn.ExchangeType, true, false, false, false, nil)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"error": err,
				}).Error("Failed to declare exchange")
				continue
			}

			_, err = channel.QueueDeclare(c.QueueName, false, c.autoDeleteQueue, c.exclusiveQueue, false, nil)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"queue": c.QueueName,
					"error": err,
				}).Errorf("Failed to declare queue, will try again in 15 seconds")
				channel.Close()
				//Give it time to autodelete
				time.Sleep(15 * time.Second)
				continue
			}

			allKeysBound := true
			for _, routingKey := range c.RoutingKeys {
				err = channel.QueueBind(c.QueueName, routingKey, c.mqconn.ExchangeName, false, nil)
				if err != nil {
					logrus.WithFields(logrus.Fields{
						"queue": c.QueueName,
						"error": err,
					}).Error("Failed to bind queue")
					allKeysBound = false
					break
				}
			}
			if !allKeysBound {
				continue
			}

			prefetch := 20
			if c.mqconn.Prefetch > 0 {
				prefetch = c.mqconn.Prefetch
			}
			channel.Qos(prefetch, 0, false)

			name, _ := os.Hostname()
			pid := os.Getpid()
			ctag := fmt.Sprintf("%v-%v", name, pid)

			deliveries, err := channel.Consume(c.QueueName, ctag, false, false, false, false, nil)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"queue": c.QueueName,
					"error": err,
				}).Error("Failed to start consumer")
				continue
			}

			logrus.WithFields(logrus.Fields{
				"queue": c.QueueName,
			}).Infof("Starting to consume")

		messageLoop:
			for {
				var (
					message         interface{}
					decompressed    []byte
					messageDelivery = MessageDelivery{
					//Context: NewContext(c.mqconn),
					}
				)

				select {
				case err := <-chanError:
					if err != nil {
						logrus.Errorf("Channel exception for %s queue: %+v", c.QueueName, err)
					} else {
						//Graceful close. c.closeChan will take care of it
						logrus.Infof("Got channel graceful shutdown event before c.closeChan for %s queue", c.QueueName)

					}
					//Giving time for connection to heal
					<-time.After(1 * time.Second)
					break messageLoop

				case d := <-deliveries:

					if d.RoutingKey == "" {
						continue
					}

					if d.Exchange == "devmgmt" && d.ContentType == "" {
						d.ContentType = "application/bencode"
					}

					msg := mg()
					decompressed = d.Body
					if d.ContentEncoding != "" {
						decompressed, err = Decompress(d.Body, d.ContentEncoding)
						if err != nil {
							logrus.WithFields(logrus.Fields{
								"error": err,
							}).Warnf("Failed to decompress Delivery message")

							d.Reject(false)
							//TODO:: check if string? if so, just pass decompressed stuff
							messageChan <- MessageDelivery{Message: MessageDelivery{Message: msg}, Delivery: d}
							continue
						}
					}

					message = decompressed
					messageDelivery.Delivery = d
					if d.ContentType != "" {
						message, err = Decode(decompressed, d.ContentType, msg)
						if err != nil {
							logrus.WithFields(logrus.Fields{
								"error":         err,
								"correlationId": d.CorrelationId,
								"replyTo":       d.ReplyTo,
								"routingKey":    d.RoutingKey,
							}).Errorf("Failed to decode Delivery message")

							d.Reject(false)
							c.mqconn.ReplyError("Malformed Request", 1, err, d)
							continue
						}
					}

					messageDelivery.Message = message

					if err = d.Ack(false); err != nil {
						logrus.WithFields(logrus.Fields{
							"ex":    d.Exchange,
							"rk":    d.RoutingKey,
							"rt":    d.ReplyTo,
							"cid":   d.CorrelationId,
							"error": err,
						}).Error("Failed to ack message")
					}

					//logrus.Infof("consume:msgDev:toChan>%+v", messageDelivery)
					messageChan <- messageDelivery

				}
			}
		}
	}()

	return messageChan, nil
}
