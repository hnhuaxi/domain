package globalkey

import "github.com/go-redis/redis/v8"

type SetRecord[K, T any] interface {
	Load(k K) ([]T, bool)
	Len(k K) int
	Add(k K, adds ...T) int
	Has(k K, v T) bool
	Range(k K) chan T
	Remove(k K, dels ...T) int
	Subscribe(k K) chan T
	SubscribeAll() chan Sub[T]
}

type setRecord[K, T any] struct {
	Prefix   string
	rediscli *redis.Client
	Option   SetOption
}

func NewSetRecord[K, T any](prefix string, rediscli *redis.Client, ops ...SetOptionFunc) SetRecord[K, T] {
	var opts SetOption
	for _, op := range ops {
		op(&opts)
	}

	return &setRecord[K, T]{
		Prefix:   prefix,
		rediscli: rediscli,
		Option:   opts,
	}
}

func (setr *setRecord[K, T]) getKey(k K) Set[T] {
	var (
		pattern = DefaultPattern
		ops     []KeyOptionFunc
	)

	if setr.Option.PatternFunc != nil {
		pattern = setr.Option.PatternFunc
	}

	if setr.Option.Log != nil {
		ops = append(ops, OptLogger(setr.Option.Log))
	}

	if setr.Option.Publish {
		ops = append(ops, OptPublish(setr.Option.PublishTopic))
	}

	if setr.Option.Expires > 0 {
		ops = append(ops, OptExpires(setr.Option.Expires))
	}

	return NewSet[T](pattern(setr.Prefix, k), setr.rediscli, ops...)
}

func (setr *setRecord[K, T]) Load(k K) ([]T, bool) {
	return setr.getKey(k).Load()
}

func (setr *setRecord[K, T]) Len(k K) int {
	return setr.getKey(k).Len()
}

func (setr *setRecord[K, T]) Add(k K, adds ...T) int {
	return setr.getKey(k).Add(adds...)
}

func (setr *setRecord[K, T]) Has(k K, v T) bool {
	return setr.getKey(k).Has(v)
}

func (setr *setRecord[K, T]) Range(k K) chan T {
	return setr.getKey(k).Range()
}

func (setr *setRecord[K, T]) Remove(k K, dels ...T) int {
	return setr.getKey(k).Remove(dels...)
}

func (setr *setRecord[K, T]) Subscribe(k K) chan T {
	panic("not implemented") // TODO: Implement
}

func (setr *setRecord[K, T]) SubscribeAll() chan Sub[T] {
	panic("not implemented") // TODO: Implement
}
