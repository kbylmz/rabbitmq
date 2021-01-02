package rabbitmq

import (
	"github.com/streadway/amqp"
)

const (
	ErrorQueueSuffix = "_ERROR"
	RetryQueueSuffix = "_RETRY"
	RetryDestinationSuffix = "_TryAgain"
	RetryTimeoutInMilliseconds = 2500
	PrefetchSize = 0
	PrefetchCount = 50
	Global = false
)

type Config struct {
	ConnectionString   string
	ExchangeName	   string
	RoutingKey		   string
	Prefix	   		   string
	ExchangeType	   string
}

// RabbitMQClient ...
type Client struct {
	Conn              *amqp.Connection
	ConnectionChannel *amqp.Channel
	Config			  Config
}

type Message struct {
	Payload   []byte
}
