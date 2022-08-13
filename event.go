package domain

import (
	"context"
	"fmt"
	"reflect"
)

type EvtHandler[E any] struct {
	commandBus *CommandBus
	handle     func(ctx context.Context, event *E) error
}

func (handler *EvtHandler[E]) HandlerName() string {
	var e E
	return reflect.TypeOf(e).Name() + "EventHandler"
}

func (handler *EvtHandler[E]) NewEvent() interface{} {
	var event = new(E)
	return event
}

func (handler *EvtHandler[E]) SetCommandBus(commandBus *CommandBus) {
	handler.commandBus = commandBus
}

func (handler *EvtHandler[E]) Handle(ctx context.Context, e interface{}) error {
	if handler.handle != nil {
		event, ok := e.(*E)
		if !ok {
			panic(fmt.Sprintf("EventHander.Handle: event is not of type %T", e))
		}

		return handler.handle(ctx, event)
	}

	return nil
}

func NewEventHandler[E any](handle func(ctx context.Context, event *E) error) *EvtHandler[E] {
	return &EvtHandler[E]{
		handle: handle,
	}
}

type NoEvent struct{}

var NoEventHandler = NewEventHandler(func(ctx context.Context, cmd *NoEvent) error {
	return nil
})
