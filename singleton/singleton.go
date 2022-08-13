package singleton

import (
	"reflect"
	"sync"
)

var objects sync.Map

func New[T any](ctor func() T) T {
	var (
		t  T
		tt = reflect.TypeOf(t)
	)
	if created, ok := objects.Load(tt); ok {
		if v, ok := created.(T); ok {
			return v
		} else {
			panic("invalid instance object")
		}
	} else {
		v := ctor()
		objects.Store(tt, v)
		return v
	}
}
