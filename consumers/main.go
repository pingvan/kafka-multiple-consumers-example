package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const N = 3

type consumer struct {
	id     int64
	reader *kafka.Reader
	wg     *sync.WaitGroup
}

func NewConsumer(id int64, groupID, topic string, brokers []string, wg *sync.WaitGroup) *consumer {
	return &consumer{
		id: id,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 10e3,
			MaxBytes: 10e6,
		}),
		wg: wg,
	}
}

func (c *consumer) consume(ctx context.Context) {
	log.Printf("run consumer with id: %v", c.id)
	defer c.wg.Done()
	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("context cancelled")
				return
			}
			log.Printf("error reading kakfa message: %v", err)
		}
		fmt.Printf("id: %d received: %s %s\n", c.id, m.Key, m.Value)
		time.Sleep(time.Millisecond * 1500)
	}
}

func main() {
	kafkaURL := os.Getenv("kafkaURL")
	groupID := os.Getenv("groupID")
	topic := os.Getenv("topic")

	log.Printf("url: %s topic: %s group: %s", kafkaURL, topic, groupID)

	brokers := strings.Split(kafkaURL, ",")
	var consumers []*consumer
	wg := sync.WaitGroup{}
	for i := range N {
		consumers = append(consumers, NewConsumer(int64(i), groupID, topic, brokers[:], &wg))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT)
	defer stop()

	wg.Add(N)
	for _, c := range consumers {
		go c.consume(ctx)
	}

	wg.Wait()
}
