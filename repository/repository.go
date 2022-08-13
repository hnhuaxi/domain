package repository

import (
	"context"

	"gorm.io/gorm"
)

type Scope = *gorm.DB

type Model[E any] interface {
	ToEntity() E
	FromEntity(entity E) interface{}
}

type ScopeFunc func(tx Scope) Scope

// Repository is a data repository interface
type Repository[M Model[E], E any] interface {
	Insert(ctx context.Context, entity *E, opts ...PutOptFunc) error
	Get(ctx context.Context, id Key, opts ...SearchOptFunc) (E, error)
	Find(ctx context.Context, opts ...SearchOptFunc) ([]E, SearchMetadata, error)
	Delete(ctx context.Context, entity E) error
}

type Builder[M Model[E], E any] interface {
	DefaultsOpts(method string, opts SearchOpt) Builder[M, E]
	SetValidKey(key ...string) Builder[M, E]
	AddFilter(id string, op Oper) Builder[M, E]
	AddCustomFilter(id string, fn CustomOpFunc) *Builder[M, E]
	AddSort(field string, defOrder ...OrderDirection) *Builder[M, E]
	AddCustomSort(field string, fn CustomSortFunc, defOrder ...OrderDirection) Builder[M, E]
	AddRelation(association string, refDef RelationDef) Builder[M, E]
	AddRelationJoin(association string, query ...string) Builder[M, E]
	AddRelationPreload(association string, query ...string) *Builder[M, E]
}

type FilterItem struct {
	ID    string
	Value interface{}
}

type SortMode struct {
	Field     string
	Direction OrderDirection
}

type OrderDirection string

const (
	OrderAuto OrderDirection = "auto"
	OrderAsc  OrderDirection = "asc"
	OrderDesc OrderDirection = "desc"
)

type FieldItem struct {
	Name  string
	Table string
}

type RelationItem struct {
	Association string
	Args        []interface{}
}
