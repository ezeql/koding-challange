package common

import (
	"fmt"
	"github.com/streadway/amqp"
)

type RabbitMQConnector struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func BuildRabbitMQConnector(host string, port int, user string, password string) (*RabbitMQConnector, error) {
	var err error
	mq := &RabbitMQConnector{}
	Info("connecting to RabbitMQ")

	url := fmt.Sprintf("amqp://%s:%s@%s:%v", user, password, host, port)

	if mq.conn, err = amqp.Dial(url); err != nil {
		return nil, err
	}
	if mq.channel, err = mq.conn.Channel(); err != nil {
		return nil, err
	}
	if err = mq.channel.ExchangeDeclare("logs", "fanout", true, false, false, false, nil); err != nil {
		return nil, err
	}
	if err = mq.channel.Qos(1, 0, false); err != nil {
		return nil, err
	}
	return mq, nil
}

func (m *RabbitMQConnector) Handle(tag string, f func([]byte) bool) error {

	q, err := m.channel.QueueDeclare(tag, true, false, false, false, nil)
	if err != nil {
		return err
	}

	if err = m.channel.QueueBind(q.Name, "", "logs", false, nil); err != nil {
		return err
	}

	msgs, err := m.channel.Consume(tag, tag, false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		Info("closing: %s", <-m.conn.NotifyClose(make(chan *amqp.Error)))
		//TODO: try to recconect
	}()
	m.tag = tag

	go handle(msgs, f, m.done)
	return nil
}

func handle(deliveries <-chan amqp.Delivery, f func([]byte) bool, done chan error) {
	for d := range deliveries {
		requeue := !f(d.Body)
		if requeue {
			Log("error processing an element: requeueing")
		}
		d.Ack(requeue)
	}
	Info("handle: deliveries channel closed")
	done <- nil
}

func (c *RabbitMQConnector) Close() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}
	defer Info("AMQP shutdown OK")
	// wait for handle() to exit
	return <-c.done
}
