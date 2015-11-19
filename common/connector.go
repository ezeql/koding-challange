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

type consumerHandler struct {
	Queue string
	F     ProcesorFunc
}

type ProcesorFunc func([]byte) bool

type RabbitMQConnector struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	tag      string
	url      string
	exchange string
	queue    string
	done     chan error
	internal chan bool
	handlers []consumerHandler
}

func BuildRabbitMQConnector(host string, port int, user string, password string, exchange string) (*RabbitMQConnector, error) {
	var err error
	mq := &RabbitMQConnector{}
	mq.url = fmt.Sprintf("amqp://%s:%s@%s:%v", user, password, host, port)
	mq.exchange = exchange

	if err = mq.open(); err != nil {
		return nil, err
	}

	go func() {
		Info("rabbitmq connection closed: ", <-mq.conn.NotifyClose(make(chan *amqp.Error)))
		ticker := time.NewTicker(time.Second * 5)
		for range ticker.C {
			Log("trying to recover connection to rabbitmq")
			if e := mq.open(); e != nil {
				Log("cannot connect to RabbitMQ at:", mq.url)
				continue
			}
			mq.rehandle()
			ticker.Stop()
		}
	}()

	return mq, err
}

func (m *RabbitMQConnector) Handle(queueName string, f ProcesorFunc) error {
	m.consume(queueName, f)
	m.handlers = append(m.handlers, consumerHandler{queueName, f})
	return nil
}

func (m *RabbitMQConnector) Close() error {
	// will close() the deliveries channel
	// if err := m.channel.Cancel(m.tag, true); err != nil {
	// 	return fmt.Errorf("Consumer cancel failed: %s", err)
	// }
	if err := m.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}
	defer Info("AMQP shutdown OK")
	// wait for handle() to exit
	return <-m.done
}

func (m *RabbitMQConnector) open() error {
	var err error

	Log("connecting to RabbitMQ")
	if m.conn, err = amqp.Dial(m.url); err != nil {
		return fmt.Errorf("Dial: %v", err)
	}
	if m.channel, err = m.conn.Channel(); err != nil {
		return fmt.Errorf("Channel: %v", err)
	}
	if err = m.channel.ExchangeDeclare(m.exchange, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("ExchangeDeclare : %v", err)
	}
	if err = m.channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("Qos : %v", err)
	}
	return nil
}

func (m *RabbitMQConnector) rehandle() error {
	for _, v := range m.handlers {
		m.consume(v.Queue, v.F)
	}
	return nil
}

func (m *RabbitMQConnector) consume(queueName string, f ProcesorFunc) error {
	_, err := m.channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return err
	}
	if err = m.channel.QueueBind(queueName, "", m.exchange, false, nil); err != nil {
		return err
	}
	msgs, err := m.channel.Consume(queueName, "", false, false, false, false, nil)

	if err != nil {
		return err
	}
	go process(msgs, f, m.done)
	return nil
}

func process(deliveries <-chan amqp.Delivery, f ProcesorFunc, done chan error) {
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

func init() {
	expvar.Publish("hitsPerSecond", hitsPerSecond)
}
