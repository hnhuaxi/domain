package kafka

import (
	"context"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/creasty/defaults"
	"github.com/hnhuaxi/domain"
	"github.com/hnhuaxi/platform/logger"
)

type SubscribeConfig struct {
	Brokers     []string
	Group       string
	OffsetReset string `default:"earliest"`
	Logger      *logger.Logger
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

		var log watermill.LoggerAdapter

		if cfg.Logger != nil {
			log = domain.StdLogger(cfg.Logger)
		} else {
			log = domain.Logger
		}

		return &kafkaSubscribe{
			log: log,
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

func CreateTopic(cfg SubscribeConfig, topic string) error {
	defaults.Set(&cfg)
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": cfg.Brokers[0]})
	if err != nil {
		return err
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create topics on cluster.
	// Set Admin options to wait for the operation to finish (or at most 60s)

	_, err = a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:         topic,
			NumPartitions: 1,
		}},
		// Admin options
		kafka.SetAdminOperationTimeout(time.Minute))
	if err != nil {
		return err
	}
	return nil
}
