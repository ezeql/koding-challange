package main

import (
	"flag"
	"fmt"
	"github.com/ezeql/koding-test/common"
	"gopkg.in/mgo.v2"
	"log"
	"time"
)

var (
	rabbitHost     = flag.String("rabbit-host", "localhost", "RabbitMQ host")
	rabbitPort     = flag.Int("rabbit-port", 5672, "RabbitMQ port")
	rabbitUser     = flag.String("rabbit-user", "guest", "RabbitMQ username")
	rabbitPassword = flag.String("rabbit-password", "guest", "RabbitMQ password")
	MongoDBHost    = flag.String("mongodb-host", "localhost", "MongoDB host")
	MongoDBPort    = flag.Int("mongodb-port", 27017, "MongoDB port")
)

func main() {
	flag.Parse()

	logs, err := common.BuildRabbitMQConnector(*rabbitHost, *rabbitPort, *rabbitUser, *rabbitPassword)
	if err != nil {
		log.Fatalln("Error on RabbitMQ:", err)
	}

	session, err := mgo.Dial(fmt.Sprintf("%s:%v", *MongoDBHost, *MongoDBPort))
	if err != nil {
		log.Fatalln("error dialing MongoDB", err)
	}
	defer session.Close()

	c := session.DB("koding").C("entries")

	logs.Handle(func(b []byte) {
		d, err := common.FromJSONBytes(b)
		if err != nil {
			panic(err)
		}
		if err = c.Insert(d); err != nil {
			log.Fatalln("cannot insert metric in mongo", err)
		}
		index := mgo.Index{
			Key:         []string{"time"},
			Name:        "time_ttl",
			ExpireAfter: time.Hour,
		}
		//delete if exists..
		c.DropIndex(index.Name)
		if err = c.EnsureIndex(index); err != nil {
			log.Fatalln("cannot create index on mongodb", err)
		}
	})
}
