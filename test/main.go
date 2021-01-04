package main

import (
	"fmt"
	"rabbitmq"
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

	//t := Test{Name: "Burak", LastName: "Yilmaz"}
	//newFsConfigBytes, _ := json.Marshal(t)
	//m := rabbitmq.Message{
	//	Payload: nil,
	//}
	//
	//if err = rc.Publish(m); err != nil {
	//	fmt.Sprint(err)
	//}

	err = rc.Consume()
	fmt.Print(err)

	fmt.Print("safa")
}
