package main

import (
	"log"
	"net/http"

	kt "github.com/bahalla/lets-chat-golang/pkg/kafka"
	"github.com/bahalla/lets-chat-golang/pkg/models"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

func main() {

	route := gin.Default()

	p := startProducer(kafka.ConfigMap{"bootstrap.servers": "192.168.1.11:30831"})

	route.GET("/send", func(c *gin.Context) {

		err := kt.Publish(*p, "my-topic", models.Message{ID: "1", User: "Taoufiq", Content: "Hello !"})

		if err == nil {
			c.JSON(http.StatusOK, gin.H{
				"message": "OK ",
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"message": "Not OK",
			})
		}
	})

	route.GET("/ws", func(ctx *gin.Context) {

		upgrader.CheckOrigin  = func(r *http.Request) bool { return true}
		ws, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
		if err != nil {
			log.Println(err)
		}
		defer ws.Close()
		log.Println("Connected !")

		for {
			var message models.Message
			err := ws.ReadJSON(&message)

			if err != nil {
				log.Printf("Reading error occured %v\n", err)
				break
			}

			log.Printf(message.Content)

			if err:= ws.WriteJSON(message); err != nil {
				log.Printf("Writing error occured %v\n", err)
			}
		}
	})


	route.Run(":9090")
}

// start kafka producer
func startProducer(conf kafka.ConfigMap) *kafka.Producer {

	p := kt.NewProducer(conf)
	go kt.HandlePublishEvents(*p)
	return p
}


