package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
)

func(c Client) Consume() error {

	chn, err := c.channel()
	if err != nil {
		return fmt.Errorf("get channel:%w",err)
	}



	messages, err := chn.Consume(
		c.config.Prefix+"."+c.config.RoutingKey, // name
		"",      // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return fmt.Errorf("Queue Consume: %s", err)
	}

	handle(messages, c)
	return nil
}

func handle(messages <-chan amqp.Delivery, c Client) {
	for msg := range messages {

		event, err := MessageDeserialize(msg.Body)
		if err != nil {
			c.HandleMessageError(msg)
			return
		}

		if event.Payload == nil {
			c.HandleMessageError(msg)
			return
		}

		fmt.Println("EVENT GELDI")

		msg.Ack(false)
	}
}

func (c *Client) consumerChannelListener(chn *amqp.Channel) {
	err := <-chn.NotifyClose(make(chan *amqp.Error))
	if err != nil && err.Code == amqp.ConnectionForced {
		fmt.Println("consumer channel listener: closed")

		if err := c.Consume(); err != nil {
			fmt.Println("HATA")
		}
	}


}


func(c Client) HandleMessageError(msg amqp.Delivery) {
	event, err := MessageDeserialize(msg.Body)

	if err != nil {
		fmt.Sprint("Message cannot be deserialized")
	}

	if event.RetryCount < 5 {
		publishRetryQueue(event, c)
	} else{
		publishErrorQueue(event, c)
	}

	msg.Nack(false, false)
}

func publishRetryQueue(m *Message, c Client)  {
	m.RetryCount++

	event,_ := json.Marshal(m)

	queueName := c.config.Prefix+"."+c.config.RoutingKey
	retryQueueName := queueName + RetryQueueSuffix

	c.ConnectionChannel.Publish(
		c.config.ExchangeName, // exchange
		retryQueueName,  // routing key
		false,                              // mandatory
		false,                              // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        event,
		})
}


func publishErrorQueue(m *Message, c Client)  {
	event,_ := json.Marshal(m)

	queueName := c.config.Prefix+"."+c.config.RoutingKey
	errorQueueName := queueName + ErrorQueueSuffix

	c.ConnectionChannel.Publish(
		c.config.ExchangeName, // exchange
		errorQueueName,  // routing key
		false,                              // mandatory
		false,                              // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        event,
		})
}
