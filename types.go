package rabbitmq

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"os"
)

const (
	ErrorQueueSuffix = "_ERROR"
	RetryQueueSuffix = "_RETRY"
	RetryDestinationSuffix = "_TryAgain"
	RetryTimeoutInMilliseconds = 25000
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
	conn              *amqp.Connection
	connectionChannel *amqp.Channel
	notifyConfirm chan amqp.Confirmation
	errorChannel   chan *amqp.Error
	done          chan os.Signal
	config			  Config
	isConnected		  bool
}

type Message struct {
	RetryCount int
	Payload   []byte
}

func MessageDeserialize(b []byte) (msg *Message, err error) {
	err = json.Unmarshal(b, &msg)
	if err != nil {
		return nil, err
	}

	return
}
