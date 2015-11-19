package main

import (
	"flag"
	"fmt"
	"github.com/ezeql/koding-challange/common"
	"github.com/garyburd/redigo/redis"
	"log"
	"net/http"
	"strconv"
	"time"
)

var (
	rabbitHost     = flag.String("rabbit-host", "127.0.0.1", "RabbitMQ host")
	rabbitPort     = flag.Int("rabbit-port", 5672, "RabbitMQ port")
	rabbitUser     = flag.String("rabbit-user", "guest", "RabbitMQ username")
	rabbitPassword = flag.String("rabbit-password", "guest", "RabbitMQ password")
	rabbitExchange = flag.String("rabbit-exchange", "logs", "RabbitMQ exchange name")
	redisHost      = flag.String("redis-host", "127.0.0.1", "redis host")
	redisPort      = flag.Int("redis-port", 6379, "redis port")
	debugMode      = flag.Bool("loglevel", false, "debug mode")
	metricsPort    = flag.Int("metrics-port", 44444, "expvar stats port")
)

const (
	bucketLength    int64 = 30 //in days
	secondsInDay    int64 = 60 * 60 * 24
	secondsInBucket int64 = secondsInDay * bucketLength
)

const (
	processQueue = "queue"
	bucketPrefix = "m_"
)

type Redis struct {
	redis.Conn
}

func main() {
	flag.Parse()
	common.DebugLevel = *debugMode

	r, err := OpenRedis(*redisHost, *redisPort)
	if err != nil {
		log.Fatalln("Cannot dial redis", err)
	}
	defer r.Close()

	connector, err := common.BuildRabbitMQConnector(*rabbitHost, *rabbitPort, *rabbitUser, *rabbitPassword, *rabbitExchange)
	if err != nil {
		log.Fatalln("cannot connect to rabbitmq", err)
	}
	defer connector.Close()

	connector.Handle("distinct-name", func(b []byte) bool {
		d := common.MustUnmarshallFromJSON(b)
		if _, err = r.processMetric(d); err != nil {
			log.Println("error inserting in redis", err)
			return false
		}
		return true
	})

	if err != nil {
		log.Fatalln("error connecting to Rabbit", err)
	}

	bindTo := fmt.Sprintf(":%v", *metricsPort)
	go http.ListenAndServe(bindTo, nil)

	for range time.Tick(time.Minute) {
		if _, err = r.processBuckets(); err != nil {
			panic(err)
		}
	}
}

func OpenRedis(host string, port int) (*Redis, error) {
	c, err := redis.Dial("tcp", fmt.Sprintf("%s:%v", host, port))
	if err != nil {
		return nil, err
	}
	r := &Redis{c}
	return r, nil
}

func (r *Redis) processMetric(m common.MetricEntry) (interface{}, error) {
	s := begginingOfDayUnix(*m.Time)
	r.Send("MULTI")
	r.Send("ZINCRBY", s, 1, m.Metric)
	r.Send("ZADD", processQueue, s, s)
	return r.Do("EXEC")
}

func (r *Redis) processBuckets() (string, error) {
	t := time.Now()
	ids, err := redis.Values(r.Do("ZRANGEBYSCORE", "queue", "-inf", begginingOfBucketUnix(t)))
	if err != nil {
		return "", err
	}
	total := len(ids)

	right := begginingOfDayUnix(t) - 1 // [....) interval
	left := right - secondsInBucket

	dest := bucketPrefix + strconv.FormatInt(begginingOfDayUnix(t), 10)

	params := []interface{}{dest, strconv.Itoa(total)}
	args := append(params, ids...)

	// //check if similar ids has been proccesed
	// //if there is a previously computed set, add to the to arguments
	// exists, err := redis.Bool(r.Do("EXISTS", dest))
	// if err != nil {
	// 	return nil, err
	// }
	// if exists {
	// 	total++
	// 	ids = append(ids, dest)
	// }
	r.Send("MULTI")
	r.Send("ZUNIONSTORE", args...)
	r.Send("ZREMRANGEBYSCORE", "queue", left, right)
	r.Send("DEL", ids...)
	_, err = r.Do("EXEC")
	return dest, err
}

func begginingOfDayUnix(t time.Time) int64 {
	tu := t.UTC().Unix()
	return tu - tu%secondsInDay
}

func begginingOfBucketUnix(t time.Time) int64 {
	tu := t.UTC().Unix()
	return tu - tu%secondsInBucket
}
