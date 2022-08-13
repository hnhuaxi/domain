package db

import (
	"context"
	"reflect"

	"github.com/akrennmair/slice"
	"github.com/hnhuaxi/domain/repository"
	"github.com/hnhuaxi/platform/logger"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type BatchRepository[M repository.Model[E], E any] interface {
	repository.Repository[M, E]
	BatchInsert(ctx context.Context, entities *[]E, opts ...repository.PutOptFunc) error
	BatchUpdate(ctx context.Context, entities *[]E, updates []string, opts ...repository.PutOptFunc) error
	BatchDelete(ctx context.Context, entities []E, opts ...repository.PutOptFunc) error
}

type DBBatchRepository[M repository.Model[E], E any] struct {
	*DBRepository[M, E]
}

func NewDBBatchRepository[M repository.Model[E], E any](db *gorm.DB, log *logger.Logger) *DBBatchRepository[M, E] {
	return &DBBatchRepository[M, E]{
		DBRepository: NewDBRepository[M, E](db, log),
	}
}

func (batch *DBBatchRepository[M, E]) BatchInsert(ctx context.Context, entities *[]E, ops ...repository.PutOptFunc) error {
	var (
		models    = SlicePb2Go[M](*entities)
		opts, err = batch.buildPutOpts(ops)
		scope     = batch.getScope()
	)
	if err != nil {
		return err
	}

	scope = batch.applyOnConflict(scope, opts)

	if opts.SkipAllAssociations {
		scope = scope.Omit(clause.Associations)
	} else if len(opts.SkipAssociations) > 0 {
		scope = scope.Omit(opts.SkipAssociations...)
	}

	if len(opts.Select) > 1 {
		first, rest := opts.Select[0], opts.Select[1:]
		scope = scope.Select(first, slice.Map(rest, func(s string) interface{} { return s })...)
	} else if len(opts.Select) > 0 {
		scope = scope.Select(opts.Select[0])
	}

	if err := scope.Create(&models).Error; err != nil {
		return err
	}

	if len(opts.ReturnKey) > 0 {
		columns := []string{"ID", "CreatedAt", "UpdatedAt"}
		columns = append(columns, opts.ReturnColumns...)
		models, err = batch.returnColumns(models, opts.ReturnKey, columns)
		if err != nil {
			return err
		}
		*entities = SliceGo2Pb[M, E](models)
	}

	return nil
}

func (batch *DBBatchRepository[M, E]) returnColumns(models []M, key string, columns []string) ([]M, error) {
	var (
		savedModels []M
		// log         = batch.logger.Sugar()
	)

	if err := batch.getScope().Clauses(batch.returnKeysExpr(models, key)).Find(&savedModels).Error; err != nil {
		return nil, err
	}

	for i, m := range savedModels {
		dst, src := reflect.ValueOf(models[i]), reflect.ValueOf(m)
		dst = reflect.Indirect(dst)
		src = reflect.Indirect(src)
		for _, col := range columns {
			// log.Infof("dst %v field %s", dst, col)
			dst.FieldByName(col).Set(src.FieldByName(col))
		}
	}

	return models, nil
}

func (batch *DBBatchRepository[M, E]) BatchUpdate(ctx context.Context, entities *[]E, updates []string) error {
	panic("nonimplement")
}

func (batch *DBBatchRepository[M, E]) BatchDelete(ctx context.Context, entities []E) error {
	panic("nonimplement")
}
