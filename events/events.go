package events

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	// "github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/AlexCuse/watermill-jetstream/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/hnhuaxi/domain"
	"github.com/hnhuaxi/domain/driver/amqp"
	"github.com/hnhuaxi/domain/driver/nsq"
	"github.com/hnhuaxi/platform/config"
	"github.com/hnhuaxi/platform/logger"
)

type Message = message.Message

type Subscriber interface {
	Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error)
}

type Publisher interface {
	Publish(topic string, msgs ...*message.Message) error
}

type Events struct {
	subscriber Subscriber
	publisher  Publisher
}

func NewEvents(config *config.Config, logger *logger.Logger) (*Events, error) {
	switch config.MessageQueue.Driver {
	case "kafka":
		var (
			cfg             = config.MessageQueue.Kafka
			subscribeConfig = kafka.DefaultSaramaSubscriberConfig()
		)

		subscriber, err := kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:               cfg.Brokers,
				Unmarshaler:           kafka.DefaultMarshaler{},
				OverwriteSaramaConfig: subscribeConfig,
				ConsumerGroup:         cfg.Group,
			},
			domain.StdLogger(logger),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka subscriber: %w", err)
		}

		publishConfig := kafka.DefaultSaramaSyncPublisherConfig()
		publishConfig.Producer.Return.Errors = true

		publisher, err := kafka.NewPublisher(
			kafka.PublisherConfig{
				Brokers:               cfg.Brokers,
				Marshaler:             kafka.DefaultMarshaler{},
				OverwriteSaramaConfig: publishConfig,
			},
			domain.StdLogger(logger),
		)

		if err != nil {
			return nil, fmt.Errorf("failed to create kafka publisher: %w", err)
		}
		return &Events{
			subscriber: subscriber,
			publisher:  publisher,
		}, nil
	case "nats":
		var (
			cfg = config.MessageQueue.Nats
		)
		_ = cfg
		// subscribeConfig := jetstream.

		subscriber, err := jetstream.NewSubscriber(
			jetstream.SubscriberConfig{
				URL:            cfg.Addr,
				CloseTimeout:   cfg.CloseTimeout,
				AckWaitTimeout: cfg.AckWaitTimeout,
				Unmarshaler:    jetstream.JSONMarshaler{},
			},
			// nats.StreamingSubscriberConfig{
			// 	ClusterID:        cfg.ClusterID,
			// 	ClientID:         cfg.ClientID,
			// 	QueueGroup:       cfg.QueueGroup,
			// 	DurableName:      cfg.DurableName,
			// 	SubscribersCount: cfg.MaxSubscribeCount, // how many goroutines should consume messages
			// 	CloseTimeout:     cfg.CloseTimeout,
			// 	AckWaitTimeout:   cfg.AckWaitTimeout,
			// 	StanOptions: []stan.Option{
			// 		stan.NatsURL(cfg.Addr),
			// 	},
			// 	Unmarshaler: nats.GobMarshaler{},
			// },
			domain.StdLogger(logger),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create nats subscriber: %w", err)
		}

		publisher, err := jetstream.NewPublisher(
			jetstream.PublisherConfig{
				URL:       cfg.Addr,
				Marshaler: jetstream.JSONMarshaler{},
			},
			domain.StdLogger(logger),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create nats publisher: %w", err)
		}

		return &Events{
			subscriber: subscriber,
			publisher:  publisher,
		}, nil
	case "nsq":
		publisher, err := nsq.NsqPublisherMaker(nsq.NsqPublisherConfig{
			Addr:      config.MessageQueue.Nsq.Addr,
			Marshaler: nsq.GobMarshaler{},
		})()
		if err != nil {
			return nil, err
		}

		subscriber, err := nsq.NsqSubscriberMaker(nsq.NsqSubscribeConfig{
			Channel:     config.MessageQueue.Nsq.Channel,
			Addr:        config.MessageQueue.Nsq.Addr,
			Unmarshaler: nsq.GobMarshaler{},
		})()
		if err != nil {
			return nil, err
		}

		return &Events{
			subscriber: subscriber,
			publisher:  publisher,
		}, nil
	case "rabbitmq":
		addr := config.MessageQueue.Rabbitmq.Addr

		publisher, err := amqp.PublisherMaker(addr)()
		if err != nil {
			return nil, err
		}

		subscriber, err := amqp.SubscriberMaker(addr)()
		return &Events{
			subscriber: subscriber,
			publisher:  publisher,
		}, nil
	default:
		return nil, domain.ErrInvalidDriverType
	}

}

func (events *Events) Subscribe(ctx context.Context, topic string) (<-chan *Message, error) {
	messages, err := events.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (events *Events) Publish(topic string, msg *Message) error {
	return events.publisher.Publish(topic, msg)
}
