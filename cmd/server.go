package main

import (
	"fmt"
	"net/http"

	kt "github.com/bahalla/lets-chat-golang/pkg/kafka"
	"github.com/bahalla/lets-chat-golang/pkg/models"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
)

func main() {

	route := gin.Default()

	p := startProducer(kafka.ConfigMap{"bootstrap.servers": "192.168.1.11:30831"})
    startConsumer(kafka.ConfigMap{"bootstrap.servers": "192.168.1.11:30831"}, "my-topic")


	route.GET("/api", func(c *gin.Context) {

		err := kt.Publish(*p, "my-topic", models.Message{ID: "1", User: "Taoufiq", Content: "Hello !"})
		
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


func startProducer(conf kafka.ConfigMap) *kafka.Producer{

	p := kt.NewProducer(conf)
	go kt.HandlePublishEvents(*p)
	return p
}

func startConsumer(conf kafka.ConfigMap, topic string) *kafka.Consumer {
	
	c := kt.NewConsumer(conf)

	if err := kt.Subscribe(*c, topic); err != nil {
		fmt.Printf("Error consuming kafka messages: %s\n", err)
	}

	return c
}