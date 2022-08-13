package repository

type Oper interface {
	Op(key, value interface{})
}

type CustomSortFunc func(scope Scope, field string, direction OrderDirection) Scope

type CustomOpFunc func(scope Scope, key, value interface{}) Scope
