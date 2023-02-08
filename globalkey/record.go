package globalkey

import (
	"context"
	"encoding/json"

	"github.com/go-redis/redis/v8"
)

type Record[K, T any] interface {
	Load(k K) (T, bool)
	Store(k K, value T) bool
	Remove(k K) bool
	Subscribe(k K) chan T
	SubscribeAll() chan Sub[T]
}

type record[K any, T any] struct {
	Prefix   string
	rediscli *redis.Client
	Option   SetOption
}

func NewRecord[K any, T any](prefix string, rediscli *redis.Client, ops ...SetOptionFunc) Record[K, T] {
	var opts SetOption
	for _, op := range ops {
		op(&opts)
	}

	return &record[K, T]{
		Prefix:   prefix,
		rediscli: rediscli,
		Option:   opts,
	}
}

func (set *record[K, T]) getKey(k K) GlobalKey[T] {
	var (
		pattern = DefaultPattern
		ops     []KeyOptionFunc
	)

	if set.Option.PatternFunc != nil {
		pattern = set.Option.PatternFunc
	}

	if set.Option.Log != nil {
		ops = append(ops, OptLogger(set.Option.Log))
	}

	if set.Option.Publish {
		ops = append(ops, OptPublish(set.Option.PublishTopic))
	}

	if set.Option.Expires > 0 {
		ops = append(ops, OptExpires(set.Option.Expires))
	}

	return NewGlobalKey[T](pattern(set.Prefix, k), set.rediscli, ops...)
}

func (set *record[K, T]) all() GlobalKey[T] {
	var (
		pattern = DefaultPattern
		ops     []KeyOptionFunc
	)

	if set.Option.PatternFunc != nil {
		pattern = set.Option.PatternFunc
	}

	return NewGlobalKey[T](pattern(set.Prefix, "*"), set.rediscli, ops...)
}

func (set *record[K, T]) Load(k K) (T, bool) {
	return set.getKey(k).Load()
}

func (set *record[K, T]) Store(k K, value T) bool {
	return set.getKey(k).Store(value)
}

func (set *record[K, T]) Remove(k K) bool {
	return set.getKey(k).Remove()
}

func (set *record[K, T]) Subscribe(k K) chan T {
	return set.getKey(k).Subscribe()
}

func (set *record[K, T]) SubscribeAll() chan Sub[T] {
	var (
		pattern = DefaultPattern
		ctx     = context.Background()
		ch      = make(chan Sub[T])
	)

	if set.Option.PatternFunc != nil {
		pattern = set.Option.PatternFunc
	}

	pubsub := set.rediscli.PSubscribe(ctx, pattern(set.Prefix, "*"))
	go func() {
		for {
			var sub Sub[T]
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				return
			}

			sub.Topic = msg.Channel
			if err = json.Unmarshal([]byte(msg.Payload), &sub.Value); err != nil {
				return
			}

			ch <- sub
		}
	}()

	return ch
}
