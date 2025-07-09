package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const N = 100000000

func main() {
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")

	conn, err := kafka.Dial("tcp", kafkaURL)
	if err != nil {
		log.Panicf("error dialing: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		log.Panicf("error creating controller: %v", err)
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		log.Panicf("error dialing controller: %v", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     4,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		log.Panicf("error creating controller: %v", err)
	}

	log.Printf("url: %s topic: %s", kafkaURL, topic)
	w := kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT)
	defer stop()
	if err != nil {
		log.Fatalf("error during wiriting init msg: %v", err)
		return
	}

	log.Println("start producing")

	for i := range N {
		key := fmt.Sprintf("Key-%d", i)
		err := w.WriteMessages(ctx,
			kafka.Message{
				Key:   []byte(key),
				Value: fmt.Append(nil, uuid.New()),
			},
		)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("error writing message: %v", err)
		}
		log.Printf("produced key: %s\n", key)
		time.Sleep(1 * time.Second)
	}
}
