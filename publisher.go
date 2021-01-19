package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func (c *Client) SafePublish(message Message) error {

	event,err := json.Marshal(message)

	if err != nil {
		return fmt.Errorf("Event cannot be published: %s", err)
	}

	channel, err := c.channel()
	if err != nil {
		return errors.New("failed to open channel")
	}
	defer channel.Close()

	if err := channel.Confirm(false); err != nil {
		return errors.New("failed to put channel in confirmation mode")
	}

	if err := channel.Publish(
		c.config.ExchangeName, // exchange
		c.config.RoutingKey,  // routing key
		false,                              // mandatory
		false,                              // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        event,
		}); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

	select {
		case ntf := <-channel.NotifyPublish(make(chan amqp.Confirmation, 1)):
			if !ntf.Ack {
				return errors.New("failed to deliver message to exchange/queue")
			}
		case <-channel.NotifyReturn(make(chan amqp.Return)):
			return errors.New("failed to deliver message to exchange/queue")
		case <-time.After(c.ChannelNotifyTimeout):
			log.Println("message delivery confirmation to exchange/queue timed out")
	}

	return nil
}

func (c *Client) UnSafePublish(message Message) error {

	event,err := json.Marshal(message)

	if err != nil {
		return fmt.Errorf("Event cannot be published: %s", err)
	}

	if err := c.ConnectionChannel.Publish(
		c.config.ExchangeName, // exchange
		c.config.RoutingKey,  // routing key
		false,                              // mandatory
		false,                              // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        event,
		}); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

	return nil
}
