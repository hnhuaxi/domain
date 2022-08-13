package domain

import (
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

type Pubsublisher interface {
	Publisher
	Subscriber
}

type (
	PublisherMaker      func() (Publisher, error)
	SubscriberMaker     func() (Subscriber, error)
	PubsublisherMaker   func() (Pubsublisher, error)
	CommandHandlerMaker func(commandBus *CommandBus, eventBus *EventBus) CommandHandler
	EventHandlerMaker   func(commandBus *CommandBus, eventBus *EventBus) EventHandler
)

func GopublisherMaker(config gochannel.Config) PublisherMaker {
	return func() (message.Publisher, error) {
		pubSub := gochannel.NewGoChannel(
			config,
			Logger,
		)

		return pubSub, nil
	}
}

func GosubscriberMaker(config gochannel.Config) SubscriberMaker {
	return func() (message.Subscriber, error) {
		pubSub := gochannel.NewGoChannel(
			config,
			Logger,
		)

		return pubSub, nil
	}
}

func GoPubsublisherMaker(config gochannel.Config) (PublisherMaker, SubscriberMaker) {
	pubSub := gochannel.NewGoChannel(
		config,
		Logger,
	)

	return func() (Publisher, error) {
			return pubSub, nil
		}, func() (Subscriber, error) {
			return pubSub, nil
		}
}

func KafkaPublisherMaker(cfg kafka.PublisherConfig) PublisherMaker {
	publishConfig := kafka.DefaultSaramaSyncPublisherConfig()

	// publishConfig.Producer.RequiredAcks = sarama.WaitForLocal
	publishConfig.Producer.Return.Errors = true
	// publishConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	// publishConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	return func() (Publisher, error) {
		return kafka.NewPublisher(
			kafka.PublisherConfig{
				Brokers:               cfg.Brokers,
				Marshaler:             cfg.Marshaler,
				OverwriteSaramaConfig: publishConfig,
			},
			Logger,
		)
	}
}

func KafkaSubscriberMaker(cfg kafka.SubscriberConfig) SubscriberMaker {
	subscribeConfig := kafka.DefaultSaramaSubscriberConfig()
	subscribeConfig.Consumer.Return.Errors = true
	subscribeConfig.Consumer.Offsets.Initial = -2
	subscribeConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange

	return func() (Subscriber, error) {
		return kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:               cfg.Brokers,
				Unmarshaler:           cfg.Unmarshaler,
				OverwriteSaramaConfig: subscribeConfig,
				ConsumerGroup:         cfg.ConsumerGroup,
			},
			Logger,
		)
	}
}
