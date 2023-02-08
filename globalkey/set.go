package globalkey

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/akrennmair/slice"
	"github.com/go-redis/redis/v8"
)

type Set[T any] interface {
	Load() ([]T, bool)
	Len() int
	Add(...T) int
	Has(T) bool
	Range() chan T
	Remove(...T) int
	Subscribe() chan T
}

type set[T any] struct {
	key      string
	rediscli *redis.Client
}

func NewSet[T any](key string, rediscli *redis.Client, ops ...KeyOptionFunc) Set[T] {
	var opts KeyOption
	for _, op := range ops {
		op(&opts)
	}

	return &set[T]{
		key:      key,
		rediscli: rediscli,
	}
}

func (set *set[T]) instance() (out T, isPtr bool) {
	var (
		z    T
		orgv = reflect.ValueOf(z)
		v    = reflect.Indirect(orgv)
	)

	isPtr = orgv.Kind() == reflect.Ptr

	switch v.Kind() {
	case reflect.Struct:
		out = reflect.New(v.Type()).Interface().(T)
	case reflect.Map:
		out = reflect.MakeMap(v.Type()).Interface().(T)
	default:
		out = reflect.New(v.Type()).Elem().Interface().(T)
	}
	return
}

func (set *set[T]) Load() ([]T, bool) {
	var (
		ctx = context.Background()
	)

	ss, err := set.rediscli.SMembers(ctx, set.key).Result()
	if err != nil {
		return nil, false
	}

	vals := slice.Map(ss, func(s string) T {
		var val, isPtr = set.instance()
		if isPtr {
			_ = json.Unmarshal([]byte(s), val)
		} else {
			_ = json.Unmarshal([]byte(s), &val)
		}
		return val
	})

	return vals, true
}

func (set *set[T]) Len() int {
	var (
		ctx = context.Background()
	)
	count, err := set.rediscli.SCard(ctx, set.key).Result()
	if err != nil {
		return 0
	}

	return int(count)
}

func (set *set[T]) Has(val T) bool {
	var (
		ctx  = context.Background()
		b, _ = json.Marshal(val)
	)
	v, err := set.rediscli.SIsMember(ctx, set.key, b).Result()
	if err != nil {
		return false
	}

	return v
}

func (set *set[T]) Add(add ...T) int {
	var (
		ctx  = context.Background()
		vals = slice.Map(add, func(a T) interface{} {
			b, _ := json.Marshal(a)
			return b
		})
	)

	c, _ := set.rediscli.SAdd(ctx, set.key, vals...).Result()
	return int(c)
}

func (set *set[T]) Remove(del ...T) int {
	var (
		ctx  = context.Background()
		vals = slice.Map(del, func(a T) interface{} {
			b, _ := json.Marshal(a)
			return b
		})
	)
	c, _ := set.rediscli.SRem(ctx, set.key, vals).Result()
	return int(c)

}

func (set *set[T]) Subscribe() chan T {
	panic("not implemented") // TODO: Implement
}

func (set *set[T]) Range() chan T {
	var (
		ctx    = context.Background()
		cursor uint64
		keys   []string
		ch     = make(chan T)
	)

	go func() {
		keys, cursor, _ = set.rediscli.SScan(ctx, set.key, cursor, "", 0).Result()
		for _, key := range keys {
			var val, isPtr = set.instance()
			if isPtr {
				_ = json.Unmarshal([]byte(key), val)
			} else {
				_ = json.Unmarshal([]byte(key), &val)
			}
			ch <- val
		}

		for cursor != 0 {
			keys, cursor, _ = set.rediscli.SScan(ctx, set.key, cursor, "", 0).Result()
			for _, key := range keys {
				var val, isPtr = set.instance()
				if isPtr {
					_ = json.Unmarshal([]byte(key), val)
				} else {
					_ = json.Unmarshal([]byte(key), &val)
				}
				ch <- val
			}
		}
	}()
	return ch
}
