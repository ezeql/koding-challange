package common

import (
	"expvar"
	"fmt"
	"github.com/ezeql/ratecounter"
	"github.com/streadway/amqp"
	"time"
)

var (
	counts        = expvar.NewMap("rabbitmq_counters")
	hitsPerSecond = ratecounter.NewRateCounter(1 * time.Second)
)

type RabbitMQConnector struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	tag      string
	url      string
	done     chan error
	internal chan bool
	handler  func([]byte) bool
}

func BuildRabbitMQConnector(host string, port int, user string, password string) (*RabbitMQConnector, error) {
	var err error
	mq := &RabbitMQConnector{}

	mq.url = fmt.Sprintf("amqp://%s:%s@%s:%v", user, password, host, port)

	if err = mq.open(); err != nil {
		return nil, err
	}
	return mq, err
}

func (m *RabbitMQConnector) open() error {
	Log("connecting to RabbitMQ")

	var err error
	if m.conn, err = amqp.Dial(m.url); err != nil {
		return err
	}
	if m.channel, err = m.conn.Channel(); err != nil {
		return err
	}
	if err = m.channel.ExchangeDeclare("logs", "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	if err = m.channel.Qos(1, 0, false); err != nil {
		return err
	}

	go func() {
		Info("rabbitmq connection closed: ", <-m.conn.NotifyClose(make(chan *amqp.Error)))
		ticker := time.NewTicker(time.Second * 5)
		for range ticker.C {
			Log("trying to recover connection to rabbitmq")
			if e := m.open(); e != nil {
				Log("cannot connect to RabbitMQ at:", m.url)
				continue
			}
			if e := m.Handle(m.tag, m.handler); e != nil {
				continue
			}
			ticker.Stop()
		}
	}()

	return nil
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

	m.tag = tag
	m.handler = f

	go handle(msgs, f, m.done)
	return nil
}

func handle(deliveries <-chan amqp.Delivery, f func([]byte) bool, done chan error) {
	for d := range deliveries {
		hitsPerSecond.Incr(1)
		requeue := !f(d.Body)
		if requeue {
			Log("error processing an element: requeueing")
			counts.Add("worker_errors", 1)
		}
		d.Ack(requeue)
	}
	Info("handle: deliveries channel closed")
	done <- nil
}

func (m *RabbitMQConnector) Close() error {
	// will close() the deliveries channel
	if err := m.channel.Cancel(m.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}
	if err := m.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}
	defer Info("AMQP shutdown OK")
	// wait for handle() to exit
	return <-m.done
}

func init() {
	expvar.Publish("hitsPerSecond", hitsPerSecond)
}
