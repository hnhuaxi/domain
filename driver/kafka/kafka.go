package kafka

import (
	"context"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/creasty/defaults"
	"github.com/hnhuaxi/domain"
)

type SubscribeConfig struct {
	Brokers     []string
	Group       string
	OffsetReset string `default:"earliest"`
}

func SubscriberMaker(cfg SubscribeConfig) domain.SubscriberMaker {
	return func() (domain.Subscriber, error) {
		defaults.Set(&cfg)

		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": strings.Join(cfg.Brokers, ","),
			"group.id":          cfg.Group,
			"auto.offset.reset": cfg.OffsetReset,
		})

		if err != nil {
			return nil, err
		}

		return &kafkaSubscribe{
			log: domain.Logger,
			c:   c,
		}, nil
	}
}

type kafkaSubscribe struct {
	log domain.LoggerAdapter
	c   *kafka.Consumer
}

func (sub *kafkaSubscribe) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	var ch = make(chan *message.Message, 1)

	go func() {
		for {
			msg, err := sub.c.ReadMessage(-1)
			if err != nil {
				sub.log.Error("read message error", err, nil)
				continue
				// return
			} else {
				// The client will automatically try to recover from all errors.
				sub.c.CommitOffsets([]kafka.TopicPartition{msg.TopicPartition})
				ch <- message.NewMessage(watermill.NewUUID(), msg.Value)

			}
		}
	}()

	if err := sub.c.SubscribeTopics([]string{topic}, nil); err != nil {
		return nil, err
	}

	return ch, nil
}

func (sub *kafkaSubscribe) Close() error {
	return sub.c.Close()
}
