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
	rabbitHost     = flag.String("rabbit-host", "192.168.99.100", "RabbitMQ host")
	rabbitPort     = flag.Int("rabbit-port", 5672, "RabbitMQ port")
	rabbitUser     = flag.String("rabbit-user", "guest", "RabbitMQ username")
	rabbitPassword = flag.String("rabbit-password", "guest", "RabbitMQ password")
	mongoDBHost    = flag.String("mongodb-host", "192.168.99.100", "MongoDB host")
	mongoDBPort    = flag.Int("mongodb-port", 27017, "MongoDB port")
	debugMode      = flag.Bool("loglevel", false, "debug mode")
	metricsPort    = flag.Int("metrics-port", 55555, "expvar stats port")
)

type Mongo struct {
	*mgo.Session
}

func main() {
	flag.Parse()
	common.DebugLevel = true

	mongo, err := OpenMongo(*mongoDBHost, *mongoDBPort)
	if err != nil {
		log.Fatalln("Error on Mongo:", err)
	}
	defer mongo.Close()

	connector, err := common.BuildRabbitMQConnector(*rabbitHost, *rabbitPort, *rabbitUser, *rabbitPassword)
	if err != nil {
		log.Fatalln("cannot connect to rabbitmq", err)
	}
	defer connector.Close()

	err = connector.Handle("hourly-log", func(b []byte) bool {
		d := common.MustUnmarshallFromJSON(b)
		if err = mongo.InsertMetric(d); err != nil {
			log.Println("error inserting in mongo", err)
			return false
		}
		return true
	})

	if err != nil {
		log.Fatalln("error connecting to Rabbit", err)
	}

	bindTo := fmt.Sprintf(":%v", *metricsPort)
	http.ListenAndServe(bindTo, nil)
}

func OpenMongo(host string, port int) (*Mongo, error) {
	session, err := mgo.Dial(fmt.Sprintf("%s:%v", host, port))
	if err != nil {
		return nil, err
	}
	m := &Mongo{session}
	return m, nil
}

func (m *Mongo) InsertMetric(d common.MetricEntry) error {
	c := m.DB("koding").C("entries")
	if err := c.Insert(d); err != nil {
		return err
	}
	index := mgo.Index{
		Key:         []string{"time"},
		Name:        "time_ttl",
		ExpireAfter: time.Hour,
	}
	//delete if exists..
	c.DropIndex(index.Name)
	return c.EnsureIndex(index)
}
