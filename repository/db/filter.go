package db

import (
	"fmt"

	"github.com/hnhuaxi/domain/repository"
	"gorm.io/gorm"
)

type Oper = repository.Oper
type CustomSortFunc = repository.CustomSortFunc

type ScopeWrap interface {
	WithScope(scope repository.Scope, fn func()) repository.Scope
}

type BindOP struct {
	scope repository.Scope
	op    func(key, val interface{}) repository.ScopeFunc
}

func (bind *BindOP) Op(key, value interface{}) {
	if bind.scope == nil {
		panic(fmt.Errorf("not bind scope, must call 'WithScope' method before"))
	}
	bind.scope = bind.op(key, value)(bind.scope)
}

func (bind *BindOP) WithScope(scope Scope, fn func()) Scope {
	bind.scope = scope
	fn()
	return bind.scope
}

var (
	EQ    = &BindOP{op: DBEquals}
	GT    = &BindOP{op: DBGreat}
	GE    = &BindOP{op: DBGreatEqual}
	LT    = &BindOP{op: DBLess}
	LE    = &BindOP{op: DBLessEqual}
	IN    = &BindOP{op: DBIN}
	LIKE  = &BindOP{op: DBLike}
	ISNIL = &BindOP{op: DBIsNil}
)

func (db *DBRepository[M, E]) RegistryOp(id string, op Oper) {
	db.filterOps[id] = op
}

func DBEquals(key, value interface{}) repository.ScopeFunc {
	return func(tx *gorm.DB) *gorm.DB {
		return tx.Where(fmt.Sprintf("%s = ?", key), value)
	}
}

func DBGreat(key, value interface{}) repository.ScopeFunc {
	return func(tx *gorm.DB) *gorm.DB {
		return tx.Where(fmt.Sprintf("%s > ?", key), value)
	}
}

func DBGreatEqual(key, value interface{}) repository.ScopeFunc {
	return func(tx *gorm.DB) *gorm.DB {
		return tx.Where(fmt.Sprintf("%s >= ?", key), value)
	}
}

func DBLessEqual(key, value interface{}) repository.ScopeFunc {
	return func(tx *gorm.DB) *gorm.DB {
		return tx.Where(fmt.Sprintf("%s <= ?", key), value)
	}
}

func DBLess(key, value interface{}) repository.ScopeFunc {
	return func(tx *gorm.DB) *gorm.DB {
		return tx.Where(fmt.Sprintf("%s < ?", key), value)
	}
}

func DBLike(key, value interface{}) repository.ScopeFunc {
	return func(tx repository.Scope) repository.Scope {
		v := fmt.Sprintf("%%%s%%", value)
		return tx.Where(fmt.Sprintf("%s LIKE ?", key), v)
	}
}

func DBIN(key, value interface{}) repository.ScopeFunc {
	return func(tx repository.Scope) repository.Scope {
		return tx.Where(fmt.Sprintf("%s IN (?)", key), value)
	}
}

func DBIsNil(key, value interface{}) repository.ScopeFunc {
	return func(tx repository.Scope) repository.Scope {
		return tx.Where(fmt.Sprintf("%s IS NULL", key))
	}
}

type Opfunc struct {
	op func(key, val interface{}) repository.ScopeFunc
}

func (op *Opfunc) Op(key, val interface{}) repository.ScopeFunc {
	return op.op(key, val)
}

type CustomOpFunc = repository.CustomOpFunc

func CustomFilter(fn CustomOpFunc) Oper {
	return &CustomOP{do: fn}
}

type CustomOP struct {
	scope repository.Scope
	do    CustomOpFunc
}

func (c *CustomOP) Op(key interface{}, value interface{}) {
	c.scope = c.do(c.scope, key, value)
}

func (c *CustomOP) WithScope(scope repository.Scope, fn func()) repository.Scope {
	c.scope = scope
	fn()
	return c.scope
}
