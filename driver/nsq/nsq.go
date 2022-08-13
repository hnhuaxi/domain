package nsq

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/hnhuaxi/domain"
	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
)

type NsqSubscriber struct {
	config  *NsqSubscribeConfig
	logger  domain.LoggerAdapter
	doneCtx context.Context
	cancel  context.CancelFunc
}

type NsqSubscribeConfig struct {
	// Topic       string
	Channel     string
	Addr        string
	Unmarshaler Unmarshaler
}

type NsqEventHandler struct {
	msg       chan *nsq.Message
	consumer  *nsq.Consumer
	unmarshal Unmarshaler
}

func NsqSubscriberMaker(cfg NsqSubscribeConfig) domain.SubscriberMaker {

	return func() (domain.Subscriber, error) {
		doneCtx, cancel := context.WithCancel(context.Background())
		sub := &NsqSubscriber{
			config:  &cfg,
			doneCtx: doneCtx,
			cancel:  cancel,
			logger:  domain.Logger,
		}

		return sub, nil
	}
}

func (sub *NsqSubscriber) newSubscribe(ctx context.Context, topic string) (*NsqEventHandler, error) {
	config := nsq.NewConfig()

	consumer, err := nsq.NewConsumer(topic, sub.config.Channel, config)
	if err != nil {
		return nil, err
	}

	var handler = &NsqEventHandler{
		msg:       make(chan *nsq.Message, 10),
		unmarshal: sub.config.Unmarshaler,
		consumer:  consumer,
	}

	consumer.AddHandler(handler)

	// Use nsqlookupd to discover nsqd instances.
	// See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
	err = consumer.ConnectToNSQD(sub.config.Addr)
	if err != nil {
		return nil, err
	}

	go func() {
		select {

		case <-ctx.Done():
			sub.logger.Trace("on nsq subscriber close", watermill.LogFields{})
			consumer.Stop()
			close(handler.msg)
		case <-sub.doneCtx.Done():
			sub.logger.Trace("on nsq subscriber all close", watermill.LogFields{})
			consumer.Stop()
			close(handler.msg)
			// case
		}

	}()

	return handler, nil
}

func (event *NsqEventHandler) HandleMessage(m *nsq.Message) error {
	event.msg <- m
	return nil
}

// Subscribe returns output channel with messages from provided topic.
// Channel is closed, when Close() was called on the subscriber.
//
// To receive the next message, `Ack()` must be called on the received message.
// If message processing failed and message should be redelivered `Nack()` should be called.
//
// When provided ctx is cancelled, subscriber will close subscribe and close output channel.
// Provided ctx is set to all produced messages.
// When Nack or Ack is called on the message, context of the message is canceled.
func (sub *NsqSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	var msgch = make(chan *message.Message)

	handler, err := sub.newSubscribe(ctx, topic)
	if err != nil {
		return nil, err
	}
	go func() {
		for msg := range handler.msg {
			mmsg, err := handler.unmarshal.Unmarshal(msg)
			if err != nil {
				sub.logger.Error("unmarshal nsq message to message.Message error", err, watermill.LogFields{})
				continue
			}
			msgch <- mmsg
			// msg.Payload
		}
	}()
	return msgch, nil
}

// Close closes all subscriptions with their output channels and flush offsets etc. when needed.
func (nsq *NsqSubscriber) Close() error {
	if nsq.cancel != nil {
		nsq.cancel()
	}

	return nil
}

type NsqPublisher struct {
	config   *NsqPublisherConfig
	producer *nsq.Producer
	logger   domain.LoggerAdapter
}

type NsqPublisherConfig struct {
	Addr      string
	Marshaler Marshaler
}

func NsqPublisherMaker(cfg NsqPublisherConfig) domain.PublisherMaker {
	return func() (domain.Publisher, error) {
		config := nsq.NewConfig()
		producer, err := nsq.NewProducer(cfg.Addr, config)
		if err != nil {
			return nil, err
		}

		return &NsqPublisher{
			config:   &cfg,
			producer: producer,
			logger:   domain.Logger,
		}, nil
	}
}

// Publish publishes provided messages to given topic.
//
// Publish can be synchronous or asynchronous - it depends on the implementation.
//
// Most publishers implementations don't support atomic publishing of messages.
// This means that if publishing one of the messages fails, the next messages will not be published.
//
// Publish must be thread safe.
func (nsq *NsqPublisher) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		messageFields := watermill.LogFields{
			"message_uuid": msg.UUID,
			"topic_name":   topic,
		}

		nsq.logger.Trace("Publishing message", messageFields)

		b, err := nsq.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return err
		}

		if err := nsq.producer.Publish(topic, b); err != nil {
			return errors.Wrap(err, "sending message failed")
		}
	}

	return nil
}

// Close should flush unsent messages, if publisher is async.
func (nsq *NsqPublisher) Close() error {
	go nsq.producer.Stop()
	return nil
}
