package nats

import (
	"github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/hnhuaxi/domain"
)

type (
	StreamingPublisherConfig  = nats.StreamingPublisherConfig
	StreamingSubscriberConfig = nats.StreamingSubscriberConfig
	GobMarshaler              = nats.GobMarshaler
)

func NatsSubscriberMaker(cfg nats.StreamingSubscriberConfig) domain.SubscriberMaker {
	return func() (domain.Subscriber, error) {
		return nats.NewStreamingSubscriber(cfg, domain.Logger)
	}
}

func NatsPublisherMaker(cfg nats.StreamingPublisherConfig) domain.PublisherMaker {

	return func() (domain.Publisher, error) {
		return nats.NewStreamingPublisher(
			cfg,
			domain.Logger,
		)
	}
}
