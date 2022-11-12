package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/bahalla/lets-chat-golang/pkg/models"
)

func NewProducer(config kafka.ConfigMap) *kafka.Producer {

	p, err := kafka.NewProducer(&config)

	if err != nil {
		panic(err)
	}
	return p
}

func HandlePublishEvents(p kafka.Producer) {

	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
			} else {
				fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
					*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
			}
		}
	}
}

func Publish(p kafka.Producer, topic string, message models.Message) error {

	err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(message.User),
		Value:          []byte(message.Content),
	}, nil)

	return err
}
