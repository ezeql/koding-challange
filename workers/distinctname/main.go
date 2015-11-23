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
	common.Info("disctinct name Worker")
	common.Info(`collects daily occurrences of distinct events in Redis. 
				Metrics that are older than 30 days are merged into a monthly bucket, 
				then cleared.`)

	common.Info("connecting to Redis...")

	r, err := OpenRedis(*redisHost, *redisPort)
	if err != nil {
		log.Fatalln("Cannot dial redis", err)
	}
	defer r.Close()
	common.Info("Connected")
	common.Info("connecting to RabbitMQ...")

	connector, err := common.BuildRabbitMQConnector(*rabbitHost, *rabbitPort, *rabbitUser, *rabbitPassword, *rabbitExchange)
	if err != nil {
		log.Fatalln("cannot connect to rabbitmq", err)
	}
	defer connector.Close()
	common.Info("connected")
	common.Info("Starting worker proccesor")

	connector.Handle("distinct-name", func(b []byte) bool {
		d := common.MustUnmarshallFromJSON(b)
		if _, err = r.processMetric(d); err != nil {
			log.Println("error inserting in redis", err)
			return false //requeue
		}
		return true
	})

	if err != nil {
		log.Fatalln("error connecting to Rabbit", err)
	}
	common.Info("Starting a metrics http server")

	bindTo := fmt.Sprintf(":%v", *metricsPort)
	go http.ListenAndServe(bindTo, nil)

	common.Info("Starting old buckets ticker")
	for range time.Tick(time.Minute) {
		if _, err = r.processBuckets(); err != nil {
			log.Println("error proccesing buckets:", err)
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
	//increments the occurrences for the given metric
	r.Send("ZINCRBY", s, 1, m.Metric)
	//adds metrics unix day number(begginingOfDayUnix) to the process queue.
	//example: if metric occured at 18th nov 9pm UTC (1447095600)
	// it will add the day identified by begginingOfDayUnix(m.Time)
	//to the process queue

	r.Send("ZADD", processQueue, s, s)
	return r.Do("EXEC")
}

func (r *Redis) processBuckets() (string, error) {
	t := time.Now()
	//get all elements which correpond to an older bucket.
	//NOTE: currently supports just a single element
	ids, err := redis.Values(r.Do("ZRANGEBYSCORE", "queue", "-inf", begginingOfBucketUnix(t)))
	if err != nil {
		return "", err
	}
	total := len(ids)

	if total == 0 { //nothing to process
		return "", nil
	}

	//name the proccesed bucket
	dest := bucketPrefix + strconv.FormatInt(begginingOfDayUnix(t), 10)

	// //check if similar ids have been proccesed
	// //if there is a previously computed set, add to the arguments
	// exists, err := redis.Bool(r.Do("EXISTS", dest))
	// if err != nil {
	// 	return nil, err
	// }
	// if exists {
	// 	total++
	// 	ids = append(ids, dest)
	// }

	r.Send("MULTI")

	params := []interface{}{dest, strconv.Itoa(total)}
	args := append(params, ids...)

	//sum all daily based occurences (loaded form the queue set) and save the result at 'dest'
	r.Send("ZUNIONSTORE", args...)

	right := begginingOfDayUnix(t) - 1 // [....) interval
	left := right - secondsInBucket

	//delete all the days corresponding to the bucket from the processing queue
	r.Send("ZREMRANGEBYSCORE", "queue", left, right)

	//delete all day-based keys
	r.Send("DEL", ids...)
	_, err = r.Do("EXEC")
	return dest, err
}

//begginingOfDayUnix returns beggining of day(as unix time) for the given time.Time
func begginingOfDayUnix(t time.Time) int64 {
	tu := t.UTC().Unix()
	return tu - tu%secondsInDay
}

func begginingOfBucketUnix(t time.Time) int64 {
	tu := t.UTC().Unix()
	return tu - tu%secondsInBucket
}
