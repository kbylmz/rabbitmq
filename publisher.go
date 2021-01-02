package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
)

func(c Client) Publish(message Message) error {

	if err := c.ConnectionChannel.Confirm(false); err != nil {
		return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	confirms := c.ConnectionChannel.NotifyPublish(make(chan amqp.Confirmation, 1))

	if err := c.ConnectionChannel.Publish(
		c.Config.ExchangeName, // exchange
		c.Config.RoutingKey,  // routing key
		false,                              // mandatory
		false,                              // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message.Payload,
		}); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

	defer func(confirms <-chan amqp.Confirmation) error {
		if confirmed := <-confirms; confirmed.Ack {
			fmt.Printf("confirmed delivery of delivery tag: %d", confirmed.DeliveryTag)
			return nil
		} else {
			return fmt.Errorf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
		}
	}(confirms)

	return nil
}
