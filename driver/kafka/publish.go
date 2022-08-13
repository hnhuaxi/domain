package kafka

import (
	"strings"

	"go.uber.org/atomic"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hnhuaxi/domain"
)

type PublisherConfig struct {
	Brokers []string
}

func PublisherMaker(cfg PublisherConfig) domain.PublisherMaker {
	return func() (domain.Publisher, error) {
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": strings.Join(cfg.Brokers, ",")})
		if err != nil {
			panic(err)
		}

		if err != nil {
			return nil, err
		}

		return &kafkaPublisher{
			log: domain.Logger,
			p:   p,
		}, nil
	}
}

type kafkaPublisher struct {
	p     *kafka.Producer
	log   domain.LoggerAdapter
	start atomic.Bool
}

func (pub *kafkaPublisher) init() {
	if pub.start.Load() {
		return
	}

	pub.start.Store(true)

	go func() {
		for e := range pub.p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					pub.log.Debug("Delivery failed:", watermill.LogFields{"Partition": ev.TopicPartition})
				} else {
					pub.log.Debug("Delivered message to:", watermill.LogFields{"Partition": ev.TopicPartition})
				}
			}
		}
	}()
}

// Publish must be thread safe.
func (pub *kafkaPublisher) Publish(topic string, messages ...*message.Message) error {
	pub.init()

	for _, msg := range messages {
		if err := pub.p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(msg.Payload),
		}, nil); err != nil {
			return err
		}
	}
	return nil
}

// Close should flush unsent messages, if publisher is async.
func (pub *kafkaPublisher) Close() error {
	pub.p.Close()
	return nil
}
