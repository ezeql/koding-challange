package main

import (
	"flag"
	"fmt"
	"github.com/ezeql/koding-challange/common"
	"gopkg.in/mgo.v2"
	"log"
	"net/http"
	"time"
)

var (
	rabbitHost     = flag.String("rabbit-host", "127.0.0.1", "RabbitMQ host")
	rabbitPort     = flag.Int("rabbit-port", 5672, "RabbitMQ port")
	rabbitUser     = flag.String("rabbit-user", "guest", "RabbitMQ username")
	rabbitPassword = flag.String("rabbit-password", "guest", "RabbitMQ password")
	rabbitExchange = flag.String("rabbit-exchange", "logs", "RabbitMQ exchange name")
	mongoDBHost    = flag.String("mongodb-host", "127.0.0.1", "MongoDB host")
	mongoDBPort    = flag.Int("mongodb-port", 27017, "MongoDB port")
	debugMode      = flag.Bool("loglevel", false, "debug mode")
	metricsPort    = flag.Int("metrics-port", 55555, "expvar stats port")
)

type mongo struct {
	*mgo.Session
}

var index = mgo.Index{
	Key:         []string{"time"},
	Name:        "time_ttl",
	ExpireAfter: time.Hour,
}

func main() {
	flag.Parse()
	common.DebugLevel = *debugMode
	common.Info("AccountName Worker")
	common.Info("collects all items that occurred in the last hour into MongoDB")
	common.Info("connecting to Mongo...")

	m, err := OpenMongo(*mongoDBHost, *mongoDBPort)
	if err != nil {
		log.Fatalln("Error on Mongo:", err)
	}
	defer m.Close()
	common.Info("Connected")
	common.Info("connecting to RabbitMQ...")

	connector, err := common.BuildRabbitMQConnector(*rabbitHost, *rabbitPort, *rabbitUser, *rabbitPassword, *rabbitExchange)
	if err != nil {
		log.Fatalln("cannot connect to rabbitmq", err)
	}
	defer connector.Close()

	common.Info("connected")
	common.Info("Starting worker proccesor")

	err = connector.Handle("hourly-log", func(b []byte) bool {
		d := common.MustUnmarshallFromJSON(b)
		if err = m.InsertMetric(d); err != nil {
			log.Println("error inserting in mongo", err)
			return false //requeue
		}
		return true
	})

	if err != nil {
		log.Fatalln("error connecting to Rabbit", err)
	}

	common.Info("Starting a metrics http server")

	bindTo := fmt.Sprintf(":%v", *metricsPort)
	http.ListenAndServe(bindTo, nil)
}

func OpenMongo(host string, port int) (*mongo, error) {
	session, err := mgo.Dial(fmt.Sprintf("%s:%v", host, port))
	if err != nil {
		return nil, err
	}
	m := &mongo{session}
	return m, nil
}

func (m *mongo) InsertMetric(d common.MetricEntry) error {
	c := m.DB("koding").C("entries")
	if err := c.EnsureIndex(index); err != nil {
		return err
	}
	return c.Insert(d)
}
