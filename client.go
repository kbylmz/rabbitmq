package rabbitmq

import (
	"github.com/streadway/amqp"
	"log"
)


//NewRabbitMQClient ...
func NewRabbitMQClient(config Config) (*Client, error) {
	conn, err := amqp.Dial(config.ConnectionString)

	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	if err = ch.Qos(PrefetchCount, PrefetchSize, Global); err != nil {
		return nil, err
	}

	if err = declareExchange(config, ch); err != nil {
		return nil, err
	}

	return &Client{
		Conn: conn,
		ConnectionChannel: ch,
		Config: config,
	}, nil
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

	queueName := c.Config.Prefix+"."+c.Config.RoutingKey
	errorQueueName := queueName + ErrorQueueSuffix
	retryQueueName := queueName + RetryQueueSuffix
	retryDestinationRoutingKey := queueName + RetryDestinationSuffix

	c.queueDeclare(queueName, nil)
	c.queueDeclare(retryQueueName, map[string]interface{} {
		"x-message-ttl": RetryTimeoutInMilliseconds,
		"x-dead-letter-exchange": c.Config.ExchangeName,
		"x-dead-letter-routing-key": retryDestinationRoutingKey,
	})
	c.queueDeclare(errorQueueName, nil)

	c.queueBind(queueName, c.Config.RoutingKey)
	c.queueBind(queueName, retryDestinationRoutingKey)
	c.queueBind(retryQueueName, retryQueueName)
	c.queueBind(errorQueueName, errorQueueName)

	return nil
}

func(c Client) queueDeclare(r string, args map[string]interface{})  {
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
		panic(err)
	}
}

func(c Client) queueBind(q, r string)  {
	err := c.ConnectionChannel.QueueBind(
		q,                           // queue name
		r,                       // routing key
		c.Config.ExchangeName, // exchange
		false,
		nil)

	if err != nil {
		log.Printf("Failed to bind queue %s to routing key %s with error %s", q, r, err)
		panic(err)
	}
}