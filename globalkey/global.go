package globalkey

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/go-redis/redis/v8"
)

type GlobalKey[T any] interface {
	Load() (T, bool)
	Store(T) bool
	Remove() bool
	Subscribe() chan T
}

type globalKey[T any] struct {
	key      string
	rediscli *redis.Client
	Option   KeyOption
}

func NewGlobalKey[T any](key string, rediscli *redis.Client, ops ...KeyOptionFunc) GlobalKey[T] {
	var opts KeyOption
	for _, op := range ops {
		op(&opts)
	}

	return &globalKey[T]{
		key:      key,
		rediscli: rediscli,
		Option:   opts,
	}
}

func (key *globalKey[T]) debug(f string, args ...interface{}) {
	if key.Option.Log != nil {
		log := key.Option.Log.Sugar()
		log.Debugf(f, args...)
	}
}

func (key *globalKey[T]) instanceValue() (instance any, isPtr bool) {
	var (
		val T
		t   = reflect.TypeOf(val)
	)

	switch t.Kind() {
	case reflect.Float32, reflect.Float64, reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64,
		reflect.Bool, reflect.String:
		return val, false
	case reflect.Ptr:
		v := reflect.New(t.Elem()).Interface()
		return v, true
	case reflect.Map:
		v := reflect.MakeMap(t).Interface()
		return v, false
	default:
		return val, false
	}
}

func (key *globalKey[T]) Load() (T, bool) {
	var (
		ctx        = context.Background()
		opts       = key.Option
		z          T
		val, isPtr = key.instanceValue()
	)

	v, err := key.rediscli.GetEx(ctx, key.key, opts.Expires).Result()
	if err != nil {
		return z, false
	}

	if isPtr {
		if err := json.Unmarshal([]byte(v), val); err != nil {
			return z, false
		}
		vv, ok := val.(T)
		return vv, ok
	} else {
		if err := json.Unmarshal([]byte(v), &z); err != nil {
			return z, false
		}
		return z, true
	}
}

func (key *globalKey[T]) Store(value T) bool {
	var (
		ctx  = context.Background()
		opts = key.Option
		err  error
	)

	b, err := json.Marshal(value)
	if err != nil {
		return false
	}

	err = key.rediscli.Set(ctx, key.key, b, opts.Expires).Err()
	if opts.Publish {
		key.publish(value)
	}
	return err == nil
}

func (key *globalKey[T]) Remove() bool {
	var (
		ctx = context.Background()
	)

	_, err := key.rediscli.Del(ctx, key.key).Result()
	return err == nil
}

func (key *globalKey[T]) Subscribe() chan T {
	var (
		ctx   = context.Background()
		ch    = make(chan T)
		topic = key.key
	)

	if len(key.Option.PublishTopic) > 0 {
		topic = key.Option.PublishTopic
	}
	pubsub := key.rediscli.Subscribe(ctx, topic)
	go func() {
		var value T
		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				return
			}

			if err = json.Unmarshal([]byte(msg.Payload), &value); err != nil {
				return
			}

			ch <- value
		}
	}()

	return ch
}

func (key *globalKey[T]) publish(val T) error {
	var (
		ctx   = context.Background()
		topic = key.key
	)

	if len(key.Option.PublishTopic) > 0 {
		topic = key.Option.PublishTopic
	}

	b, err := json.Marshal(val)
	if err != nil {
		return err
	}

	key.debug("publish %v to key %s", val, topic)
	_, err = key.rediscli.Publish(ctx, topic, b).Result()
	return err
}
