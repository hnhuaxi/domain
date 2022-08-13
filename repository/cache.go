package repository

import (
	"context"

	"github.com/hnhuaxi/cache"
	"github.com/hnhuaxi/platform/logger"
)

type CacheRepository[M Model[E], E any] struct {
	logger *logger.Logger
	repos  Repository[M, E]
	cache  *cache.Cache[string, E]
}

func NewCacheRepository[M Model[E], E any](repos Repository[M, E], logger *logger.Logger, size int) *CacheRepository[M, E] {
	var log = logger.Sugar()
	cache, err := cache.New[string, E](size)
	if err != nil {
		log.Fatalf("create cache failed with size: %d, because %s", size, err)
	}
	return &CacheRepository[M, E]{
		logger: logger,
		repos:  repos,
		cache:  cache,
	}
}

func (cache *CacheRepository[M, E]) Put(ctx context.Context, entity E) error {
	panic("not implemented") // TODO: Implement
}

func (cache *CacheRepository[M, E]) Insert(ctx context.Context, entity E, opts ...PutOptFunc) error {
	panic("not implemented") // TODO: Implement
}

func (cache *CacheRepository[M, E]) Peek(ctx context.Context, id Key) (E, error) {
	panic("not implemented") // TODO: Implement
}

func (cache *CacheRepository[M, E]) Get(ctx context.Context, id Key, opts ...SearchOptFunc) (E, error) {
	panic("not implemented") // TODO: Implement
}

func (cache *CacheRepository[M, E]) Find(ctx context.Context, opts ...SearchOptFunc) ([]E, SearchMetadata, error) {
	panic("not implemented") // TODO: Implement
}

func (cache *CacheRepository[M, E]) Delete(ctx context.Context, entity E) error {
	panic("not implemented") // TODO: Implement
}
