package kafka

import (
	"fmt"
	"os"
	"time"

	"github.com/bahalla/lets-chat-golang/pkg/models"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewConsumer(config kafka.ConfigMap) *kafka.Consumer {

	c, err := kafka.NewConsumer(&config)
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}
	return c
}

func Subscribe(c kafka.Consumer, topic string) (models.Message, error) {

	err := c.SubscribeTopics([]string{topic}, nil)

	ev, err := c.ReadMessage(100 * time.Millisecond)
	msg := models.Message{}
	if ev != nil {
		msg = models.Message{ID: "1", User: string(ev.Key), Content: string(ev.Value)}
	}

	return msg, err
}
