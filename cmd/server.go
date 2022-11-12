package main

import (
	"net/http"

	kproducer "github.com/bahalla/lets-chat-golang/pkg/kafka"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
)

func main() {
	route := gin.Default()

	p := kproducer.NewProducer(kafka.ConfigMap{"bootstrap.servers": "192.168.1.11:9094"})

	go kproducer.HandlePublishEvents(*p)

	route.GET("/api", func(c *gin.Context) {

		err := kproducer.Publish(*p, "my-topic", "Hello")
		if err == nil {
			c.JSON(http.StatusOK, gin.H{
				"message": "Message sent",
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"message": "Message sent",
			})
		}
	})

	route.Run(":9090")
}
