package registry

import "reflect"

type Registry[T any] struct {
	set map[reflect.Type]Ctor[T]
}

type Ctor[T any] func() T

func (reg *Registry[T]) init() {
	if reg.set == nil {
		reg.set = make(map[reflect.Type]Ctor[T])
	}
}

func (reg *Registry[T]) Register(node interface{}, ctor Ctor[T]) {
	reg.init()

	reg.set[reflect.ValueOf(node).Type()] = ctor
}

func (reg *Registry[T]) Lookup(node interface{}) (ctor Ctor[T], ok bool) {
	reg.init()

	ctor, ok = reg.set[reflect.ValueOf(node).Type()]
	return ctor, ok
}
