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

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func main() {

	route := gin.Default()

	p := startProducer(kafka.ConfigMap{"bootstrap.servers": "192.168.1.11:30831"})

	hub := NewHub()
	go hub.run()

	route.GET("/ws", func(ctx *gin.Context) {

		upgrader.CheckOrigin = func(r *http.Request) bool { return true }
		ws, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
		if err != nil {
			log.Println(err)
		}
		defer func() {
			delete(hub.clients, ws)
			ws.Close()
			log.Printf("Closed!")
		}()
		// Add client
		hub.clients[ws] = true

		log.Println("Connected!")

		// Listen on connection
		read(hub, ws, p)
	})

	route.Run(":9090")
}

// start kafka producer
func startProducer(conf kafka.ConfigMap) *kafka.Producer {

	p := kt.NewProducer(conf)
	go kt.HandlePublishEvents(*p)
	return p
}

func read(hub *Hub, client *websocket.Conn, p *kafka.Producer) {
	for {
		var message models.Message
		err := client.ReadJSON(&message)
		if err != nil {
			log.Printf("error occurred: %v", err)
			delete(hub.clients, client)
			break
		}
		log.Println(message)

		err = kt.Publish(*p, "my-topic", message)
		if err != nil {
			log.Printf("error occurred writing to kafka: %v", err)
			delete(hub.clients, client)
			break
		}

		// Send a message to hub
		hub.broadcast <- message
	}
}
