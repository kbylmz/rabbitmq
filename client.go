package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)


//NewRabbitMQClient ...
func NewRabbitMQClient(config Config) (*Client, error) {

	client := &Client{
		config: config,
		ChannelNotifyTimeout: config.ChannelNotifyTimeout,
	}

	return client, nil
}

func(c Client) Setup()  error {
	channel, err := c.Channel()
	if err != nil {
		return errors.New("failed to open channel")
	}
	defer channel.Close()

	if err = c.declareExchange(channel); err != nil {
		return err
	}

	if err = c.initializeQueues(channel); err != nil {
		return err
	}

	return nil
}

func(c Client) declareExchange(ch *amqp.Channel) (err error) {
	err = ch.ExchangeDeclare(
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

func(c Client) initializeQueues(channel *amqp.Channel) error {

	queueName := c.config.Prefix+"."+c.config.RoutingKey
	errorQueueName := queueName + ErrorQueueSuffix
	retryQueueName := queueName + RetryQueueSuffix
	retryDestinationRoutingKey := queueName + RetryDestinationSuffix

	c.queueDeclare(queueName, nil, channel)
	c.queueDeclare(retryQueueName, map[string]interface{} {
		"x-message-ttl": RetryTimeoutInMilliseconds,
		"x-dead-letter-exchange": c.config.ExchangeName,
		"x-dead-letter-routing-key": retryDestinationRoutingKey,
	}, channel)
	c.queueDeclare(errorQueueName, nil, channel)

	c.queueBind(queueName, c.config.RoutingKey, channel)
	c.queueBind(queueName, retryDestinationRoutingKey, channel)
	c.queueBind(retryQueueName, retryQueueName, channel)
	c.queueBind(errorQueueName, errorQueueName, channel)

	return nil
}

func(c Client) queueDeclare(r string, args map[string]interface{}, channel *amqp.Channel) error  {
	_, err := channel.QueueDeclare(
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

func(c Client) queueBind(q, r string, channel *amqp.Channel)  error {
	err := channel.QueueBind(
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
				fmt.Println("INFO: Reconnected")

				goto WATCH
			}

			time.Sleep(2 * time.Second)
		}
	} else {
		log.Println("INFO: Connection dropped normally, will not reconnect")
	}

	return nil
}

func (c *Client) Connect() error{

	conn, err := amqp.Dial(c.config.ConnectionString)
	if err != nil {
		return err
	}

	c.conn = conn

	go c.handleReconnect()

	return nil

}

func (c *Client) Channel() (*amqp.Channel, error) {
	if c.conn == nil {
		if err := c.Connect(); err != nil {
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
