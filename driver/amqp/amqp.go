package amqp

import (
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/hnhuaxi/domain"
)

func SubscriberMaker(addr string) domain.SubscriberMaker {
	return func() (domain.Subscriber, error) {
		commandsAMQPConfig := amqp.NewDurableQueueConfig(addr)
		commandsSubscriber, err := amqp.NewSubscriber(commandsAMQPConfig, domain.Logger)
		if err != nil {
			return nil, err
		}

		return commandsSubscriber, nil
	}
}

func PublisherMaker(addr string) domain.PublisherMaker {
	return func() (domain.Publisher, error) {
		commandsAMQPConfig := amqp.NewDurableQueueConfig(addr)
		commandsPublisher, err := amqp.NewPublisher(commandsAMQPConfig, domain.Logger)
		if err != nil {
			return nil, err
		}

		return commandsPublisher, nil
	}
}
