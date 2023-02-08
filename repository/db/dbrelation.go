package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/akrennmair/slice"
	"github.com/hnhuaxi/domain"
	"github.com/hnhuaxi/domain/repository"
	"github.com/hnhuaxi/domain/utils"
	"github.com/hnhuaxi/platform/logger"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type DBRelationRepository[A repository.Model[E], E any, B repository.Model[T], T any] struct {
	*DBRepository[B, T]

	db         *gorm.DB
	Assocation string

	schemaA *schema.Schema
	schemaB *schema.Schema
}

func NewDBRelationRepository[A repository.Model[E], E any, B repository.Model[T], T any](db *gorm.DB, log *logger.Logger, assocation string) *DBRelationRepository[A, E, B, T] {
	return &DBRelationRepository[A, E, B, T]{
		DBRepository: NewDBRepository[B, T](db, log),
		db:           db,
		Assocation:   assocation,
	}
}

func (r *DBRelationRepository[A, E, B, T]) Append(ctx context.Context, elem E, targets ...T) error {
	var (
		scope = r.getScope()
		start A
		m     = start.FromEntity(elem).(A)
	)

	associations := SlicePb2Any[B](targets)
	if err := scope.Model(m).Association(r.Assocation).Append(associations...); err != nil && !domain.CheckDuplicateRelation(err) {
		return err
	}
	return nil
}

func (r *DBRelationRepository[A, E, B, T]) Find(ctx context.Context, elem E, opts ...SearchOptFunc) ([]T, repository.SearchMetadata, error) {
	var (
		scope    = r.getScope()
		start    A
		target   B
		m        = start.FromEntity(elem).(A)
		err      error
		metadata repository.SearchMetadata
	)

	var (
		models = make([]B, 0, 100)
	)

	scope, err = r.joinRelatedModel(ctx, scope, m, target)
	if err != nil {
		return nil, metadata, err
	}

	sch, err := r.getSchemaB()
	if err != nil {
		return nil, metadata, err
	}

	so, err := r.getSearchOptions("Find", opts, sch)
	if err != nil {
		return nil, metadata, err
	}

	log.Printf("search_options %+v", so)
	withField := func(key string, do func(field *schema.Field)) (_err error) {
		defer func() {
			if err, ok := recover().(error); ok {
				_err = err
			}
		}()

		field, err := r.Field(key)
		if err != nil {
			return fmt.Errorf("repository: invalid field %w", err)
		}
		do(field)
		return nil
	}

	withKey := func(key repository.Key, do func(field *schema.Field)) error {
		if key == nil {
			return nil
		}
		return withField(key.Name(), do)
	}

	// 分页处理阶段
	if err := withKey(so.Page.AfterId, func(field *schema.Field) {
		scope = scope.Where(fmt.Sprintf("%s > ?", field.DBName), so.Page.AfterId.Value())
	}); err != nil {
		return nil, metadata, err
	}

	if err := withKey(so.Page.BeforeId, func(field *schema.Field) {
		scope = scope.Where(fmt.Sprintf("%s < ?", field.DBName), so.Page.BeforeId.Value())
	}); err != nil {
		return nil, metadata, err
	}

	scope = scope.Limit(so.Page.PageSize)
	if so.Page.Page > 0 {
		scope = scope.Offset((so.Page.Page - 1) * so.Page.PageSize)
	}
	metadata.Page = so.Page.Page
	metadata.PageSize = so.Page.PageSize

	// 载入过滤器阶段
	if err := r.validFilters(so.Filters); err != nil {
		return nil, metadata, err
	}

	if scope, err = ChainErr(so.Filters, scope, func(filter repository.FilterItem, scope Scope) (Scope, error) {
		id := utils.SnakeCase(filter.ID)
		field, err := r.Field(id)
		if err != nil {
			return nil, err
		}
		typeoper, ok := r.filterOps[field.Name]
		if !ok {
			return nil, fmt.Errorf("no register filter id %s", filter.ID)
		}

		// if bindOp, ok := op.(*BindOP); ok {
		// 	scope = bindOp.WithScope(scope, func() {
		// 		bindOp.Op(field.DBName, filter.Value)
		// 	})
		// } else {
		// 	return nil, fmt.Errorf("invalid db op bind of %s", filter.ID)
		// }
		if bindOp, ok := typeoper.Oper.(ScopeWrap); ok {
			filter.Type = typeoper.Type
			scope = bindOp.WithScope(scope, func() {
				typeoper.Oper.Op(field.DBName, filter.Val())
			})
		} else {
			return nil, fmt.Errorf("invalid db op bind of %s", filter.ID)
		}
		return scope, nil
	}); err != nil {
		return nil, metadata, err
	}

	// 排序处理阶段
	so.Sorts = r.defaultSorts(so.Sorts)
	if err := r.validSorts(so.Sorts); err != nil {
		return nil, metadata, err
	}

	scope = Chain(so.Sorts, scope, func(sort repository.SortMode, scope Scope) Scope {
		name, _ := r.DBName(sort.Field)
		return scope.Order(fmt.Sprintf("%s %s", name, sort.Direction))
	})

	// 处理字段列表
	fields := slice.Filter(so.Fields, func(fi repository.FieldItem) bool {
		return fi.Table == "" || fi.Table == sch.Table
	})
	fieldNames := slice.Map(fields, func(fi repository.FieldItem) string {
		return fi.Name
	})
	scope = scope.Select(fieldNames)

	if len(so.Select) > 0 {
		scope = scope.Select(so.Select)
	}

	// 关联载入
	if scope, err = ChainErr(so.Relations, scope, func(relation repository.RelationItem, scope Scope) (Scope, error) {
		refDef, ok := r.relations[relation.Association]
		if !ok {
			return nil, fmt.Errorf("no register association %s", relation.Association)
		}

		switch {
		case refDef.Join:
			if len(refDef.Query) > 0 && (refDef.ArgsCount) > 0 {
				return scope.Joins(refDef.Query, relation.Args[:refDef.ArgsCount]), nil
			} else {
				return scope.Joins(relation.Association), nil
			}
		default:
			if len(refDef.Query) > 0 && (refDef.ArgsCount) > 0 {
				args := []interface{}{refDef.Query}
				args = append(args, relation.Args[:refDef.ArgsCount]...)
				return scope.Preload(relation.Association, args...), nil
			} else {
				return scope.Preload(relation.Association), nil
			}
		}
	}); err != nil {
		return nil, metadata, err
	}

	// 扩展 db scopes 处理
	for _, dbScope := range so.DBScopes {
		scope = dbScope(scope)
	}

	// 获取 total 数据
	scope = r.applyTotal(ctx, scope)

	if err := r.withDebug(ctx, scope, func(scope Scope) Scope {
		return scope.Find(&models)
	}); err != nil {
		return nil, metadata, err
	}

	if total, ok := GetScopeTotal(scope); ok {
		if err := total.Total(&metadata.Total); err != nil {
			return nil, metadata, err
		}
	}

	r.applyMetadata(&metadata, models)

	return SliceGo2Pb[B, T](models), metadata, nil
}

func (r *DBRelationRepository[A, E, B, T]) Replace(ctx context.Context, elem E, targets ...T) error {
	var (
		scope = r.getScope()
		start A
		m     = start.FromEntity(elem).(A)
	)

	associations := SlicePb2Any[B](targets)
	if err := scope.Model(m).Association(r.Assocation).Replace(associations...); err != nil {
		return err
	}

	return nil
}

func (r *DBRelationRepository[A, E, B, T]) Delete(ctx context.Context, elem E, targets ...T) error {
	var (
		scope = r.getScope()
		start A
		m     = start.FromEntity(elem).(A)
	)

	associations := SlicePb2Any[B](targets)
	if err := scope.Model(m).Association(r.Assocation).Delete(associations...); err != nil {
		return err
	}

	return nil
}

func (r *DBRelationRepository[A, E, B, T]) AddFilter(id string, op Oper) *DBRelationRepository[A, E, B, T] {
	r.DBRepository.AddFilter(id, op)
	return r
}

func (r *DBRelationRepository[A, E, B, T]) AddCustomFilter(id string, fn CustomOpFunc) *DBRelationRepository[A, E, B, T] {
	r.DBRepository.AddCustomFilter(id, fn)
	return r
}

func (r *DBRelationRepository[A, E, B, T]) AddSort(field string, defOrder ...OrderDirection) *DBRelationRepository[A, E, B, T] {
	r.DBRepository.AddSort(field, defOrder...)
	return r
}

func (r *DBRelationRepository[A, E, B, T]) AddCustomSort(field string, fn CustomSortFunc, defOrder ...OrderDirection) *DBRelationRepository[A, E, B, T] {
	r.DBRepository.AddCustomSort(field, fn, defOrder...)
	return r
}

func (r *DBRelationRepository[A, E, B, T]) AddRelation(association string, refDef repository.RelationDef) *DBRelationRepository[A, E, B, T] {
	r.DBRepository.AddRelation(association, refDef)
	return r
}

func (r *DBRelationRepository[A, E, B, T]) AddRelationJoin(association string, query ...string) *DBRelationRepository[A, E, B, T] {
	r.DBRepository.AddRelationJoin(association, query...)
	return r
}

func (r *DBRelationRepository[A, E, B, T]) AddRelationPreload(association string, query ...string) *DBRelationRepository[A, E, B, T] {
	r.DBRepository.AddRelationPreload(association, query...)
	return r
}

func (r *DBRelationRepository[A, E, B, T]) getSchemaA() (*schema.Schema, error) {
	var m A
	if r.schemaA == nil {
		sch, err := schema.Parse(m, &sync.Map{}, r.namer)
		if err != nil {
			return nil, err
		}
		r.schemaA = sch
	}

	return r.schemaA, nil
}

func (r *DBRelationRepository[A, E, B, T]) getSchemaB() (*schema.Schema, error) {
	var m B
	if r.schemaB == nil {
		sch, err := schema.Parse(m, &sync.Map{}, r.namer)
		if err != nil {
			return nil, err
		}
		r.schemaB = sch
	}

	return r.schemaB, nil
}

func (r *DBRelationRepository[A, E, B, T]) AField(name string) (*schema.Field, error) {
	sch, err := r.getSchemaA()
	if err != nil {
		return nil, err
	}

	if field := sch.LookUpField(name); field == nil {
		return nil, fmt.Errorf("invalid field id %s", name)
	} else {
		return field, nil
	}
}

func (r *DBRelationRepository[A, E, B, T]) AFieldName(name string) (string, error) {
	if field, err := r.AField(name); err != nil {
		return "", err
	} else {
		return field.Name, nil
	}
}

func (r *DBRelationRepository[A, E, B, T]) ADBName(name string) (string, error) {
	if field, err := r.AField(name); err != nil {
		return "", err
	} else {
		return field.DBName, nil
	}
}

func (r *DBRelationRepository[A, E, B, T]) BField(name string) (*schema.Field, error) {
	sch, err := r.getSchemaB()
	if err != nil {
		return nil, err
	}

	if field := sch.LookUpField(name); field == nil {
		return nil, fmt.Errorf("invalid field id %s", name)
	} else {
		return field, nil
	}
}

func (r *DBRelationRepository[A, E, B, T]) BFieldName(name string) (string, error) {
	if field, err := r.BField(name); err != nil {
		return "", err
	} else {
		return field.Name, nil
	}
}

func (r *DBRelationRepository[A, E, B, T]) BDBName(name string) (string, error) {
	if field, err := r.BField(name); err != nil {
		return "", err
	} else {
		return field.DBName, nil
	}
}

func (r *DBRelationRepository[A, E, B, T]) joinRelatedModel(ctx context.Context, scope Scope, elem A, target B) (Scope, error) {
	// schA, err := r.getSchemaA()
	// if err != nil {
	// 	return nil, err
	// }

	// schB, err := r.getSchemaB()
	// if err != nil {
	// 	return nil, err
	// }
	sche, err := schema.Parse(elem, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		return nil, err
	}

	relation, ok := sche.Relationships.Relations[r.Assocation]
	if !ok {
		return nil, fmt.Errorf("invalid assocation '%s'", r.Assocation)
	}

	switch relation.Type {
	case schema.HasMany, schema.HasOne:
		privalue, err := r.getPrimaryValue(elem)
		if err != nil {
			return nil, err
		}
		if len(relation.References) == 0 {
			return nil, errors.New("relation References is empty")
		}
		ref := relation.References[0]
		scope = scope.Clauses(clause.Where{
			Exprs: []clause.Expression{
				clause.Eq{Column: ref.ForeignKey.DBName, Value: privalue},
			},
		})
	case schema.BelongsTo:
		return nil, errors.New("nonimplement")
	case schema.Many2Many:
		joinExprs, err := JoinBuilder(elem, target, r.Assocation)
		if err != nil {
			return scope, err
		}

		// sch, err := schema.Parse(target, &sync.Map{}, schema.NamingStrategy{})
		// if err != nil {
		// 	return nil, err
		// }
		// // sch.Table +

		scope = scope.Clauses(joinExprs)
		// scope.Select()
	}

	// r.FieldName()
	return scope, nil
}

type join struct {
	Mode         clause.JoinType
	Table        string
	Column       string
	Target       string
	TargetColumn string
	OnExpr       string
}

func (j *join) String() string {
	return fmt.Sprintf("%s JOIN %s ON %s.%s = %s.%s", j.Mode, j.Table, j.Table, j.Column, j.Target, j.TargetColumn)
}

func SlicePb2Go[M interface {
	FromEntity(E) any
}, E any](vals []E) []M {
	return slice.Map(vals, func(v E) M {
		var m M
		return m.FromEntity(v).(M)
	})
}

func SlicePb2Any[M interface {
	FromEntity(E) any
}, E any](vals []E) []any {
	return slice.Map(vals, func(v E) any {
		var m M
		return m.FromEntity(v).(M)
	})
}

func SliceGo2Pb[M interface {
	ToEntity() E
}, E any](vals []M) []E {
	return slice.Map(vals, func(v M) E {
		return v.ToEntity()
	})
}

func RelateModel(elem, target any) ([]clause.Expression, error) {
	var exprs []clause.Expression
	aSch, err := schema.Parse(elem, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		return nil, err
	}

	bSch, err := schema.Parse(target, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		return nil, err
	}

	colA := clause.Column{
		Table: aSch.Table,
		Name:  utils.Singular(aSch.Table) + "_id",
	}
	colB := clause.Column{
		Table: bSch.Table,
		Name:  utils.Singular(bSch.Table) + "_id",
	}

	exprs = append(exprs, clause.Join{
		Type:  clause.InnerJoin,
		Table: clause.Table{Name: aSch.Table},
		ON: clause.Where{
			Exprs: []clause.Expression{clause.Eq{Column: colA, Value: colB}},
		},
	})

	if len(aSch.PrimaryFields) == 0 {
		return nil, errors.New("elem model must have primary field")
	}

	primaryField := aSch.PrimaryFields[0]
	exprs = append(exprs, clause.Where{
		Exprs: []clause.Expression{
			clause.Eq{
				Column: clause.Column{
					Table: aSch.Table,
					Name:  clause.PrimaryKey,
				}, Value: fieldValue(elem, primaryField.Name),
			},
		},
	})

	return exprs, nil
}

func fieldValue(m any, fieldName string) any {
	v := reflect.ValueOf(m)
	v = reflect.Indirect(v)
	fv := v.FieldByName(fieldName)
	if fv.IsValid() {
		return fv.Interface()
	}
	return nil
}

type JoinAssociation struct {
	Association string
	Elem        *schema.Schema
	JoinTable   *schema.Schema
	Field       *schema.Field
	Target      *schema.Schema
	TargetField *schema.Field
	fieldValue  interface{}
}

func (join *JoinAssociation) ModifyStatement(stmt *gorm.Statement) {
	from := stmt.Clauses["FROM"]
	from.AfterExpression = join
	stmt.Clauses["FROM"] = from
	// selectClause := stmt.Clauses["SELECT"]
	// if selectClause.AfterExpression == nil {
	// 	selectClause.AfterExpression = clause.Select{
	// 		Distinct: false,
	// 		Columns:  join.elemColumns(),
	// 	}
	// 	stmt.Clauses["SELECT"] = selectClause
	// }
}

func (join *JoinAssociation) elemColumns() []clause.Column {
	var columns []clause.Column
	for _, field := range join.Elem.DBNames {
		columns = append(columns, clause.Column{
			Table: join.JoinTable.Name,
			Name:  field,
		})
	}
	return columns
}

func (j *JoinAssociation) Build(build clause.Builder) {
	var (
		join = clause.Join{
			Type: clause.InnerJoin,
			Table: clause.Table{
				Name: j.JoinTable.Table,
			},
			ON: clause.Where{
				Exprs: []clause.Expression{
					clause.And(
						clause.Eq{
							Column: clause.Column{
								Table: j.JoinTable.Table,
								Name:  j.Field.DBName,
							},
							Value: clause.Column{
								Table: j.Target.Table,
								Name:  clause.PrimaryKey,
							},
						},
						clause.Eq{
							Column: clause.Column{
								Table: j.JoinTable.Table,
								Name:  j.TargetField.DBName,
							},
							Value: j.fieldValue,
						},
					),
				},
			},
		}
	)

	join.Build(build)
}

func JoinBuilder(elem, target interface{}, association string) (*JoinAssociation, error) {
	sche, err := schema.Parse(elem, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		return nil, err
	}

	scht, err := schema.Parse(target, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		return nil, err
	}

	relation, ok := sche.Relationships.Relations[association]
	if !ok {
		return nil, fmt.Errorf("can't found `%s` association", association)
	}

	var (
		joinTable = relation.JoinTable
	)

	if joinTable == nil {
		return nil, nil
	}

	firstField := utils.Singular(sche.Table) + "_id"
	secondField := utils.Singular(scht.Table) + "_id"

	id, err := GetPrimaryValue(elem)
	if err != nil {
		return nil, err
	}

	joinField := joinTable.LookUpField(secondField)

	targetField := joinTable.LookUpField(firstField)

	return &JoinAssociation{
		Association: association,
		Elem:        sche,
		JoinTable:   joinTable,
		Field:       joinField,
		Target:      scht,
		TargetField: targetField,
		fieldValue:  id,
	}, nil
}
