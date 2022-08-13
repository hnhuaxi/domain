package domain

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
)

type (
	EventBus              = cqrs.EventBus
	CommandBus            = cqrs.CommandBus
	Publisher             = message.Publisher
	Subscriber            = message.Subscriber
	Router                = message.Router
	HandlerFunc           = message.HandlerFunc
	NoPublishHandlerFunc  = message.NoPublishHandlerFunc
	RouterConfig          = message.RouterConfig
	CommandEventMarshaler = cqrs.CommandEventMarshaler
	LoggerAdapter         = watermill.LoggerAdapter
)

type CommandHandler interface {
	SetEventBus(*EventBus)
	cqrs.CommandHandler
}

type EventHandler interface {
	SetCommandBus(*CommandBus)
	cqrs.EventHandler
}
