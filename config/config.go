package config

import (
	"time"

	"github.com/inanzzz/client/internal/pkg/rabbitmq"
	"github.com/inanzzz/client/internal/user"
)

type Config struct {
	HTTPAddress string
	RabbitMQ    rabbitmq.Config
	UserAMQP    user.AMQPConfig
}

func New() Config {
	var cnf Config

	cnf.HTTPAddress = ":8080"

	cnf.RabbitMQ.Schema = "amqp"
	cnf.RabbitMQ.Username = "inanzzz"
	cnf.RabbitMQ.Password = "123123"
	cnf.RabbitMQ.Host = "0.0.0.0"
	cnf.RabbitMQ.Port = "5672"
	cnf.RabbitMQ.Vhost = "my_app"
	cnf.RabbitMQ.ConnectionName = "MY_APP"
	cnf.RabbitMQ.ChannelNotifyTimeout = 100 * time.Millisecond
	cnf.RabbitMQ.Reconnect.Interval = 500 * time.Millisecond
	cnf.RabbitMQ.Reconnect.MaxAttempt = 7200

	cnf.UserAMQP.Create.ExchangeName = "user"
	cnf.UserAMQP.Create.ExchangeType = "direct"
	cnf.UserAMQP.Create.RoutingKey = "create"
	cnf.UserAMQP.Create.QueueName = "user_create"

	return cnf
}
