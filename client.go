package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)


//NewRabbitMQClient ...
func NewRabbitMQClient(config Config) (client *Client, err error) {

	client = &Client{
		config: config,
		ChannelNotifyTimeout: config.ChannelNotifyTimeout,
	}

	if err = client.connect(); err != nil {
		return nil, err
	}

	if client.ConnectionChannel, err = client.channel(); err != nil {
		log.Fatalln(err)
	}

	return
}

func(c Client) Setup()  error {

	if err := c.declareExchange(); err != nil {
		return err
	}

	if err := c.initializeQueues(); err != nil {
		return err
	}

	return nil
}

func(c Client) declareExchange() (err error) {
	err = c.ConnectionChannel.ExchangeDeclare(
		c.config.ExchangeName,         // name
		c.config.ExchangeType, 		// type
		true,                   // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // no-wait
		nil,                    // arguments
	)

	if err != nil {
		log.Printf("Declare exchange %s is failed with error %s", c.config.ExchangeName, err)
		return
	}

	return
}

func(c Client) initializeQueues() error {

	queueName := c.config.Prefix+"."+c.config.RoutingKey
	errorQueueName := queueName + ErrorQueueSuffix
	retryQueueName := queueName + RetryQueueSuffix
	retryDestinationRoutingKey := queueName + RetryDestinationSuffix

	c.queueDeclare(queueName, nil)
	c.queueDeclare(retryQueueName, map[string]interface{} {
		"x-message-ttl": RetryTimeoutInMilliseconds,
		"x-dead-letter-exchange": c.config.ExchangeName,
		"x-dead-letter-routing-key": retryDestinationRoutingKey,
	})
	c.queueDeclare(errorQueueName, nil)

	c.queueBind(queueName, c.config.RoutingKey)
	c.queueBind(queueName, retryDestinationRoutingKey)
	c.queueBind(retryQueueName, retryQueueName)
	c.queueBind(errorQueueName, errorQueueName)

	return nil
}

func(c Client) queueDeclare(r string, args map[string]interface{}) error  {
	_, err := c.ConnectionChannel.QueueDeclare(
		r, 			// name
		true,              // durable, the queue will survive a broker restart
		false,          // delete when used
		false,            // exclusive
		false,            // no-wait
		args,              // arguments
	)

	if err != nil {
		log.Printf("Failed to declare queue %s with error %s", r, err)
		return err
	}

	return nil
}

func(c Client) queueBind(q, r string)  error {
	err := c.ConnectionChannel.QueueBind(
		q,                           // queue name
		r,                       // routing key
		c.config.ExchangeName, // exchange
		false,
		nil)
	
	if err != nil {
		log.Printf("Failed to bind queue %s to routing key %s with error %s", q, r, err)
		return err
	}

	return nil
}

func (c *Client) handleReconnect() error {
WATCH:
	conErr := <-c.conn.NotifyClose(make(chan *amqp.Error))
	if conErr != nil {
		fmt.Println("CRITICAL: Connection dropped, reconnecting")

		var err error

		for i := 1; i <= 30; i++ {
			c.conn, err = amqp.Dial(c.config.ConnectionString)

			if err == nil {
				c.ConnectionChannel, err = c.channel()
				if err == nil {
					fmt.Println("INFO: Reconnected")
					goto WATCH
				}
			}

			time.Sleep(2 * time.Second)
		}
	} else {
		log.Println("INFO: Connection dropped normally, will not reconnect")
	}

	return nil
}

func (c *Client) connect() error{

	conn, err := amqp.Dial(c.config.ConnectionString)
	if err != nil {
		return err
	}

	c.conn = conn

	go c.handleReconnect()

	return nil

}

func (c *Client) channel() (*amqp.Channel, error) {
	if c.conn == nil {
		if err := c.connect(); err != nil {
		return nil, errors.New("connection is not open")
		}
	}

	channel, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}

	return channel, nil
}

func (c *Client) Shutdown() error {
	if c.conn != nil {
		return c.conn.Close()
	}

	return nil
}
