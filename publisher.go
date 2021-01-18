package rabbitmq

import "C"
import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
)

func(c Client) Publish(message Message) error {

	event,err := json.Marshal(message)

	if err != nil {
		return fmt.Errorf("Event cannot be published: %s", err)
	}

	//if c.conn.IsClosed() {
	//	c.isConnected = false
	//	c.handleReconnect()
	//}

	//theChannel, err := c.conn.Channel()
	//if err != nil {
	//	return err
	//}

	//if err = c.connectionChannel.Confirm(false); err != nil {
	//	c.isConnected = false
	//	c.handleReconnect()
	//}


	if err := c.connectionChannel.Publish(
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

	//if err := theChannel.Close(); err != nil {
	//	return err
	//}

	return nil

	//if err = c.connectionChannel.Confirm(false); err != nil {
	//	c.isConnected = false
	//	c.handleReconnect()
	//}

	//confirms := c.notifyConfirm
	//defer confirmOne(confirms)



	
	//if confirmed := <-c.notifyConfirm; confirmed.Ack {
	//	fmt.Printf("confirmed delivery of delivery tag: %d", confirmed.DeliveryTag)
	//	return nil
	//}else {
	//	<-time.After(1000)
	//}


	return nil
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func confirmOne(confirms <-chan amqp.Confirmation) {
	fmt.Println("waiting for confirmation of one publishing")

	//if confirmed := <-confirms; confirmed.Ack {
	//	fmt.Println("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	//} else {
	//	fmt.Println("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	//}
}
