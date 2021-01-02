package main

import (
	"encoding/json"
	"fmt"
	"github.com/kbylmz/rabbitmq"
)

type Test struct {
	Name string
	LastName string
}

func main()  {

	conf := rabbitmq.Config{
		ConnectionString: "amqp://sa:Sa123456@localhost:5672/",
		Prefix: "Test",
		ExchangeName: "Merchant-Exchange",
		ExchangeType: "topic",
		RoutingKey: "Deneme.CreatedEvent",
	}

	rc, err := rabbitmq.NewRabbitMQClient(conf)

	if err != nil {
		fmt.Print(err)
	}

	err = rc.InitializeQueues()

	if err != nil {
		fmt.Print(err)
	}

	t := Test{Name: "Burak", LastName: "Yilmaz"}
	newFsConfigBytes, _ := json.Marshal(t)
	m := rabbitmq.Message{
		Payload: newFsConfigBytes,
	}

	rc.Publish(m)

}
