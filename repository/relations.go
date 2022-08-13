package repository

import "context"

// Repository is a data repository interface
type RelationRepository[A Model[E], E any, B Model[T], T any] interface {
	Append(ctx context.Context, elem E, associations ...T) error
	Find(ctx context.Context, elem E, opts ...SearchOptFunc) ([]T, error)
	Replace(ctx context.Context, elem E, associations ...T) error
	Delete(ctx context.Context, elem E, associations ...T) error
}
