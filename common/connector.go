package common

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type RabbitMQConnector struct {
	Ch   *amqp.Channel
	Q    *amqp.Queue
	Conn *amqp.Connection
}

func BuildRabbitMQConnector(host string, port int, user string, password string) (*RabbitMQConnector, error) {
	var err error
	mq := &RabbitMQConnector{}
	url := fmt.Sprintf("amqp://%s:%s@%s:%v", user, password, host, port)

	if mq.Conn, err = amqp.Dial(url); err != nil {
		return nil, err
	}
	if mq.Ch, err = mq.Conn.Channel(); err != nil {
		return nil, err
	}
	if err = mq.Ch.ExchangeDeclare("logs", "fanout", true, false, false, false, nil); err != nil {
		return nil, err
	}
	if err = mq.Ch.Qos(1, 0, false); err != nil {
		return nil, err
	}
	q, err := mq.Ch.QueueDeclare("distinctName", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	mq.Q = &q
	return mq, nil
}

func (m *RabbitMQConnector) Handle(f func([]byte)) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		defer m.close()
		msgs, err := m.Ch.Consume(m.Q.Name, "", false, false, false, false, nil)
		if err != nil {
			panic(err)
		}
		for d := range msgs {
			f(d.Body)
			if err := d.Ack(false); err != nil {
				panic(err)
			}
		}
	}()

	<-signalChan
}

func (m *RabbitMQConnector) close() {
	if err := m.Ch.Close(); err != nil {
		log.Println("error clossing ampq channel:", err)
	}
	if err := m.Conn.Close(); err != nil {
		log.Println("error clossing ampq connection:", err)
	}
}
