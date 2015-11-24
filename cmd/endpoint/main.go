package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ezeql/koding-challange/common"
	"github.com/gin-gonic/contrib/expvar"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"time"
)

var (
	rabbitHost     = flag.String("rabbit-host", "192.168.99.100", "RabbitMQ host")
	rabbitPort     = flag.Int("rabbit-port", 5672, "RabbitMQ port")
	rabbitUser     = flag.String("rabbit-user", "guest", "RabbitMQ username")
	rabbitPassword = flag.String("rabbit-password", "guest", "RabbitMQ password")
	rabbitExchange = flag.String("rabbit-exchange", "logs", "RabbitMQ exchange name")
	debugMode      = flag.Bool("loglevel", false, "debug mode")
	serverPort     = flag.Int("endpoint-port", 8080, "port")
)

type metricsService struct {
	connector *common.RabbitMQConnector
}

func main() {
	flag.Parse()

	common.Info("initializing metric http endpoint")
	connector, err := common.BuildRabbitMQConnector(*rabbitHost, *rabbitPort, *rabbitUser, *rabbitPassword, *rabbitExchange)
	if err != nil {
		log.Fatalln("error", err)
	}

	bindTo := fmt.Sprintf(":%d", *serverPort)

	common.Info("binding to:", bindTo)

	service := metricsService{connector: connector}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.POST("/metric", service.createMetric)
	r.GET("/debug/vars", expvar.Handler())

	if err := r.Run(bindTo); err != nil {
		log.Fatalln(err)
	}
}

func (service *metricsService) createMetric(c *gin.Context) {
	var entry common.MetricEntry

	if err := c.BindJSON(&entry); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		c.Abort()
		return
	}

	entry.Time = time.Now().UTC()

	b, err := json.Marshal(entry)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		c.Abort()
		return
	}

	if err := service.connector.Publish(b); err != nil {
		log.Println("error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		c.Abort()
		return
	}

	c.JSON(http.StatusOK, entry)
}
