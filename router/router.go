package router

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/hnhuaxi/domain"
)

type Router struct {
	Topic string

	logger     domain.LoggerAdapter
	router     *message.Router
	subscriber domain.Subscriber
	publisher  domain.Publisher

	listens  map[string]*HandlerStruct
	listenMu sync.RWMutex
}

type HandlerStruct struct {
	Type     reflect.Type
	Handlers []CommandHander
	mu       sync.RWMutex
}

type Message struct {
	Cmd     string
	Payload json.RawMessage
}

type CommandHander func(ctx context.Context, cmd interface{}) error

func New(config domain.RouterConfig, logger domain.LoggerAdapter) (*Router, error) {
	router, err := message.NewRouter(config, domain.Logger)
	if err != nil {
		return nil, err
	}

	return &Router{
		router:  router,
		listens: make(map[string]*HandlerStruct),
		logger:  logger,
	}, nil
}

func (r *Router) Run(ctx context.Context) error {
	return r.router.Run(ctx)
}

func (r *Router) Send(ctx context.Context, cmd interface{}) error {
	panic("nonimplement")
}

func (r *Router) AddHandler(cmd interface{}, handler CommandHander) {
	var v = reflect.ValueOf(cmd)
	v = reflect.Indirect(v)

	r.addEvent(v.Type().Name(), cmd, handler)

	r.router.AddNoPublisherHandler(
		v.Type().Name(),
		r.Topic,
		r.subscriber,
		r.processMessage,
	)
}

func (r *Router) addEvent(name string, cmd interface{}, handler CommandHander) {
	r.listenMu.Lock()
	defer r.listenMu.Unlock()

	var v = reflect.ValueOf(cmd)
	v = reflect.Indirect(v)

	handles, ok := r.listens[name]
	if !ok {
		handles = &HandlerStruct{
			Type: v.Type(),
		}
	}

	handles.AddHandler(handler)
	r.listens[name] = handles
}

func (r *Router) processMessage(rawmsg *message.Message) error {
	var msg Message
	if err := json.Unmarshal(msg.Payload, &msg); err != nil {
		return err
	}

	r.listenMu.RLock()
	handleStruct, ok := r.listens[msg.Cmd]
	if !ok {
		r.logger.Trace("invalid cmd no register", watermill.LogFields{"Command": msg.Cmd})
	}
	r.listenMu.RUnlock()

	var (
		cmd = handleStruct.NewCommand()
		ctx = context.Background()
	)

	if err := json.Unmarshal(msg.Payload, &cmd); err != nil {
		return err
	}

	return handleStruct.Do(ctx, cmd)
}

func (handler *HandlerStruct) NewCommand() interface{} {
	return reflect.New(handler.Type).Interface()
}

func (handler *HandlerStruct) AddHandler(cmdhandler CommandHander) {
	handler.mu.Lock()
	defer handler.mu.Unlock()

	handler.Handlers = append(handler.Handlers, cmdhandler)
}

func (handler *HandlerStruct) Do(ctx context.Context, cmd interface{}) error {
	handler.mu.RLock()
	defer handler.mu.RUnlock()

	for _, handler := range handler.Handlers {
		if err := handler(ctx, cmd); err != nil {
			return err
		}
	}

	return nil
}
