package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)


//NewRabbitMQClient ...
func NewRabbitMQClient(config Config) (*Client, error) {

	client := &Client{
		config: config,
		isConnected: false,
	}

	 if err := client.connect(); err != nil{
		 return nil, err
	 }


	if err := declareExchange(config, client.connectionChannel); err != nil {
		return nil, err
	}


	return client, nil
}


func declareExchange(c Config, ch *amqp.Channel) (err error) {
	err = ch.ExchangeDeclare(
		c.ExchangeName,         // name
		c.ExchangeType, 		// type
		true,                   // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // no-wait
		nil,                    // arguments
	)

	if err != nil {
		log.Printf("Declare exchange %s is failed with error %s", c.ExchangeName, err)
		return
	}

	return
}

func(c Client) InitializeQueues() error {
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()

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

func(c Client) queueDeclare(r string, args map[string]interface{})  {
	_, err := c.connectionChannel.QueueDeclare(
		r, 			// name
		true,              // durable, the queue will survive a broker restart
		false,          // delete when used
		false,            // exclusive
		false,            // no-wait
		args,              // arguments
	)

	if err != nil {
		log.Printf("Failed to declare queue %s with error %s", r, err)
		panic(err)
	}
}

func(c Client) queueBind(q, r string)  {
	err := c.connectionChannel.QueueBind(
		q,                           // queue name
		r,                       // routing key
		c.config.ExchangeName, // exchange
		false,
		nil)

	if err != nil {
		log.Printf("Failed to bind queue %s to routing key %s with error %s", q, r, err)
		panic(err)
	}
}

func (c *Client) handleReconnect() {
	for !c.isConnected {

		errC := <-c.errorChannel

		if !c.isConnected {
			fmt.Println(errC)
			err := c.connect()

			if err != nil {
				fmt.Println("RETRY CONNECT")
				time.Sleep(10*time.Second)
				continue
			}
		}

	}
	fmt.Println("Connected to rabbitMQ")
}

func (c *Client) connect() error{
	conn, err := amqp.Dial(c.config.ConnectionString)

	if err != nil {
		return err
	}

	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	c.isConnected = true
	c.conn = conn
	c.connectionChannel = ch
	c.errorChannel = make(chan *amqp.Error)
	c.notifyConfirm = make(chan amqp.Confirmation, 1)
	c.connectionChannel.NotifyClose(c.errorChannel)
	c.connectionChannel.NotifyPublish(c.notifyConfirm)

	return nil
}