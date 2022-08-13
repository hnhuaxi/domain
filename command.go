package domain

import (
	"context"
	"fmt"
	"reflect"
)

type CmdHandler[C any] struct {
	eventBus *EventBus
	handle   func(ctx context.Context, command *C) error
}

func (handler *CmdHandler[C]) HandlerName() string {
	var c C
	return reflect.TypeOf(c).Name() + "CommandHandler"
}

func (handler *CmdHandler[C]) NewCommand() interface{} {
	var command = new(C)
	return command
}

func (handler *CmdHandler[C]) SetEventBus(eventBus *EventBus) {
	handler.eventBus = eventBus
}

func (handler *CmdHandler[C]) Handle(ctx context.Context, c interface{}) error {
	if handler.handle != nil {
		command, ok := c.(*C)
		if !ok {
			panic(fmt.Sprintf("CommandHander.Handle: command is not of type %T", c))
		}
		return handler.handle(ctx, command)
	}

	return nil
}

func NewCmdHandler[C any](handle func(ctx context.Context, cmd *C) error) *CmdHandler[C] {
	return &CmdHandler[C]{
		handle: handle,
	}
}

type NoCommand struct{}

var NoCommandHandler = NewCmdHandler(func(ctx context.Context, cmd *NoCommand) error {
	return nil
})
