package messagebus

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/akrennmair/slice"
	"github.com/hnhuaxi/domain"
)

type MessageBus struct {
	facade     *cqrs.Facade
	config     BusConfig
	configDone bool
	router     *domain.Router
}

type RouterHandler struct {
	HandleName       string
	SubscribeTopic   string
	Subscriber       domain.Subscriber
	PublishTopic     string
	Publisher        domain.Publisher
	Handler          domain.HandlerFunc
	NopublishHandler domain.NoPublishHandlerFunc
	NoPublish        bool
}

type BusConfig struct {
	CommandHandlers      []domain.CommandHandler
	CommandHandlerMakers []domain.CommandHandlerMaker
	EventHandlers        []domain.EventHandler
	EventHandlerMakers   []domain.EventHandlerMaker
	RouterHandlers       []RouterHandler
	EventsName           string

	// pubsublisherMaker PubsublisherMaker
	PublisherMaker   domain.PublisherMaker
	SubscriberMaker  domain.SubscriberMaker
	RouterConfig     *domain.RouterConfig
	CommandMarshaler domain.CommandEventMarshaler
	Logger           watermill.LoggerAdapter
}

var (
	DefaultMarshaler    = domain.JSONMarshaler
	DefaultRouterConfig = &domain.RouterConfig{}
	DefaultConfig       = BusConfig{
		RouterConfig:     DefaultRouterConfig,
		Logger:           domain.Logger,
		CommandMarshaler: DefaultMarshaler,
	}
)

func NewMessageBus(config BusConfig) *MessageBus {
	if config.CommandMarshaler == nil {
		config.CommandMarshaler = DefaultMarshaler
	}

	if config.Logger == nil {
		config.Logger = domain.Logger
	}

	if config.RouterConfig == nil {
		config.RouterConfig = DefaultRouterConfig
	}

	return &MessageBus{
		config: config,
	}
}

func (bus *MessageBus) buildConfig() bool {
	if bus.configDone {
		return false
	}
	// pubsub, err := bus.config.pubsublisherMaker()
	// if err != nil {
	// 	panic(err)
	// }

	publisher, err := bus.config.PublisherMaker()
	if err != nil {
		panic(err)
	}

	eventsPublisher, err := bus.config.PublisherMaker()
	if err != nil {
		panic(err)
	}

	subscriber, err := bus.config.SubscriberMaker()
	if err != nil {
		panic(err)
	}

	router, err := message.NewRouter(*bus.config.RouterConfig, bus.config.Logger)
	if err != nil {
		panic(err)
	}

	config := cqrs.FacadeConfig{
		GenerateCommandsTopic: func(commandName string) string {
			// we are using queue RabbitMQ config, so we need to have topic per command type
			return commandName
		},
		CommandsPublisher: publisher,
		CommandsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			// we can reuse subscriber, because all commands have separated topics
			return bus.config.SubscriberMaker()
		},
		GenerateEventsTopic: func(eventName string) string {
			if bus.config.EventsName == "" {
				// because we are using PubSub RabbitMQ config, we can use one topic for all events
				return "events"
			} else {
				return bus.config.EventsName
			}
		},
		EventsPublisher: eventsPublisher,
		EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			// config := amqp.NewDurablePubSubConfig(
			return subscriber, nil
		},
		CommandEventMarshaler: bus.config.CommandMarshaler,
		Logger:                bus.config.Logger,
	}

	// if len(bus.config.CommandHandlers) == 0 || len(bus.config.EventHandlers) == 0 {
	// 	return false
	// }

	config.CommandHandlers = func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.CommandHandler {
		log.Printf("command handlers")
		var handlers = make([]cqrs.CommandHandler, 0)

		handlers = append(handlers, slice.Map(bus.config.CommandHandlers, func(cmdHandler domain.CommandHandler) cqrs.CommandHandler {
			cmdHandler.SetEventBus(eb)
			return cmdHandler
		})...)

		handlers = append(handlers, slice.Map(bus.config.CommandHandlerMakers, func(cmdHandler domain.CommandHandlerMaker) cqrs.CommandHandler {
			return cmdHandler(cb, eb)
		})...)

		return handlers
	}

	config.EventHandlers = func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.EventHandler {
		log.Printf("events handlers")
		var handlers = make([]cqrs.EventHandler, 0)

		handlers = append(handlers, slice.Map(bus.config.EventHandlers, func(evtHandler domain.EventHandler) cqrs.EventHandler {
			evtHandler.SetCommandBus(cb)
			return evtHandler
		})...)

		handlers = append(handlers, slice.Map(bus.config.EventHandlerMakers, func(eventHandler domain.EventHandlerMaker) cqrs.EventHandler {
			return eventHandler(cb, eb)
		})...)

		return handlers
	}

	config.Router = router
	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(middleware.Recoverer)
	router.AddMiddleware(middleware.Retry{
		MaxRetries:      3,
		MaxElapsedTime:  3 * time.Minute,
		InitialInterval: 10 * time.Second,
	}.Middleware)

	bus.router = router

	for _, routerHandler := range bus.config.RouterHandlers {
		if !routerHandler.NoPublish {
			bus.router.AddHandler(
				routerHandler.HandleName,
				routerHandler.SubscribeTopic,
				routerHandler.Subscriber,
				routerHandler.PublishTopic,
				routerHandler.Publisher,
				routerHandler.Handler,
			)
		} else {
			bus.router.AddNoPublisherHandler(
				routerHandler.HandleName,
				routerHandler.SubscribeTopic,
				routerHandler.Subscriber,
				routerHandler.NopublishHandler,
			)
		}
	}

	cqrsFacade, err := cqrs.NewFacade(config)
	if err != nil {
		panic(err)
	}

	bus.facade = cqrsFacade
	bus.configDone = true
	return true
}

func (bus *MessageBus) CommandBus() *cqrs.CommandBus {
	bus.buildConfig()
	return bus.facade.CommandBus()
}

func (bus *MessageBus) EventBus() *cqrs.EventBus {
	bus.buildConfig()
	return bus.facade.EventBus()
}

func (bus *MessageBus) AddCmdHandler(handler domain.CommandHandler) *MessageBus {
	bus.config.CommandHandlers = append(bus.config.CommandHandlers, handler)
	return bus
}

func (bus *MessageBus) AddCmdHandlerMaker(handler domain.CommandHandlerMaker) *MessageBus {
	bus.config.CommandHandlerMakers = append(bus.config.CommandHandlerMakers, handler)
	return bus
}

func (bus *MessageBus) AddEventHandler(handler domain.EventHandler) *MessageBus {
	bus.config.EventHandlers = append(bus.config.EventHandlers, handler)
	return bus
}

func (bus *MessageBus) AddEventHandlerMaker(handler domain.EventHandlerMaker) *MessageBus {
	bus.config.EventHandlerMakers = append(bus.config.EventHandlerMakers, handler)
	return bus
}

func (bus *MessageBus) Run(ctx context.Context) error {
	bus.buildConfig()

	return bus.router.Run(ctx)
}

func (bus *MessageBus) Subscriber() (domain.Subscriber, error) {
	bus.buildConfig()
	return bus.config.SubscriberMaker()
}

func (bus *MessageBus) Publisher() (domain.Publisher, error) {
	bus.buildConfig()
	return bus.config.PublisherMaker()
}

func (bus *MessageBus) AddRouterHandler(
	handlerName string,
	subscribeTopic string, subscriber domain.Subscriber,
	publishTopic string, publisher domain.Publisher,
	handle domain.HandlerFunc,
) {

	bus.config.RouterHandlers = append(bus.config.RouterHandlers, RouterHandler{
		HandleName:     handlerName,
		SubscribeTopic: subscribeTopic,
		Subscriber:     subscriber,
		PublishTopic:   publishTopic,
		Publisher:      publisher,
		Handler:        handle,
	})
}

func (bus *MessageBus) AddRouterNoPublishHandler(
	handlerName string,
	subscribeTopic string, subscriber domain.Subscriber,
	handle domain.NoPublishHandlerFunc,
) {
	bus.config.RouterHandlers = append(bus.config.RouterHandlers, RouterHandler{
		HandleName:       handlerName,
		SubscribeTopic:   subscribeTopic,
		Subscriber:       subscriber,
		NopublishHandler: handle,
		NoPublish:        true,
	})
}

func (bus *MessageBus) Router() *domain.Router {
	bus.buildConfig()
	return bus.router
}
