package repository

import (
	"reflect"

	"github.com/jinzhu/copier"
)

func CopyFrom[T any](src T) T {
	var (
		v     = reflect.ValueOf(src)
		val   reflect.Value
		isPtr bool
		dst   T
		ok    bool
	)

	for {
		switch v.Kind() {
		case reflect.Map:
			val = reflect.MakeMap(v.Type())
		case reflect.Struct:
			val = reflect.New(v.Type())
			val = val.Elem()
		case reflect.Ptr:
			val = reflect.Indirect(val)
			isPtr = true
			continue
		default:
			dst = src
			return dst
		}
		break
	}

	if dst, ok = val.Interface().(T); ok {
		if isPtr {
			copier.Copy(dst, src)
		} else {
			copier.Copy(&dst, src)
		}
	} else {
		panic("invalid type")
	}

	return dst
}

func FromEntity[M interface{ FromEntity(E) interface{} }, E any](entity E) M {
	var m M
	return m.FromEntity(entity).(M)
}
