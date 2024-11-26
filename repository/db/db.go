package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"

	"github.com/akrennmair/slice"
	"github.com/hnhuaxi/domain/repository"
	"github.com/hnhuaxi/domain/utils"
	"github.com/hnhuaxi/platform/logger"
	"github.com/hnhuaxi/utils/convert"
	"github.com/jinzhu/copier"
	"go.uber.org/multierr"
	"golang.org/x/exp/slices"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type (
	Scope          = repository.Scope
	PutOptFunc     = repository.PutOptFunc
	SearchOptFunc  = repository.SearchOptFunc
	OrderDirection = repository.OrderDirection
	PutOption      = repository.PutOption
	SearchOpt      = repository.SearchOpt
	KeyFunc        = repository.KeyFunc
)

type DBRepository[M repository.Model[E], E any] struct {
	logger      *logger.Logger
	db          *gorm.DB
	tx          *gorm.DB
	defaults    map[string]repository.SearchOpt
	filterOps   map[string]TypeOper
	sorts       map[string]repository.OrderDirection
	extendSorts map[string]CustomSortFunc
	relations   map[string]repository.RelationDef
	extendsIds  map[string]bool
	validKeys   []string
	schema      *schema.Schema
	inTrans     bool
	namer       schema.Namer
}

func NewDBRepository[M repository.Model[E], E any](db *gorm.DB, log *logger.Logger) *DBRepository[M, E] {
	return &DBRepository[M, E]{
		logger:      log,
		db:          db,
		defaults:    make(map[string]repository.SearchOpt),
		filterOps:   make(map[string]TypeOper),
		validKeys:   make([]string, 0),
		relations:   make(map[string]repository.RelationDef),
		extendsIds:  make(map[string]bool),
		sorts:       make(map[string]repository.OrderDirection),
		extendSorts: make(map[string]CustomSortFunc),
		namer:       schema.NamingStrategy{},
	}
}

// func (r *DBRepository[M, E]) Insert(ctx context.Context, elem *E) error {
// 	var start M
// 	m := start.FromEntity(*elem).(M)
// 	*elem = m.ToEntity()
// 	if err := r.getScope().Create(m).Error; err != nil {
// 		return nil
// 	}
// 	return nil
// }

var debugSQLKey = struct{}{}

func WithDebugSQL(ctx context.Context) context.Context {
	return context.WithValue(ctx, debugSQLKey, true)
}

func IsDebug(ctx context.Context) bool {
	val, ok := ctx.Value(debugSQLKey).(bool)
	return ok && val
}

func (r *DBRepository[M, E]) instantE() E {
	var (
		e E
		v = reflect.ValueOf(e)
		t = reflect.TypeOf(e)
	)

	if t.Kind() == reflect.Ptr {
		// log.Printf("v type %s", t.Type())
		// v = reflect.Indirect(v)
		instance := reflect.New(t.Elem())
		e, _ = instance.Interface().(E)
		return e
	} else if v.IsValid() && !v.IsNil() {
		return e
	} else {
		panic("invalid entity type")
	}
}

func (r *DBRepository[M, E]) instantM() M {
	var (
		m M
		v = reflect.ValueOf(m)
	)

	if v.Kind() == reflect.Ptr {
		instance := reflect.New(reflect.Indirect(v).Type())
		v.Set(instance)
		return m
	} else if v.IsValid() && !v.IsNil() {
		return m
	} else {
		panic("invalid model type")
	}
}

func (r *DBRepository[M, E]) Get(ctx context.Context, key repository.Key, opts ...repository.SearchOptFunc) (E, error) {
	var (
		e = r.instantE()
	)

	if err := r.GetInto(ctx, &e, key, opts...); err != nil {
		return e, err
	}

	return e, nil
}

func (r *DBRepository[M, E]) keyWhereString(key repository.Key) string {
	return fmt.Sprintf("%s = ?", key.Name())
}

func (r *DBRepository[M, E]) GetInto(ctx context.Context, entity *E, key repository.Key, opts ...repository.SearchOptFunc) error {
	var (
		scope = r.getScope()
		g     M
		m     = g.FromEntity(*entity)
		opt   *repository.SearchOpt
	)

	sch, err := r.getSchema()
	if err != nil {
		return err
	}

	opt, err = r.getSearchOptions("Get", opts, sch)
	if err != nil {
		return err
	}

	scope = r.applySearchColumns(opt, scope)

	scope = r.applyRelations(scope, opt.Relations)
	// 扩展 db scopes 处理
	for _, dbScope := range opt.DBScopes {
		scope = dbScope(scope)
	}

	return r.withDebug(ctx, scope, func(scope Scope) Scope {
		defer func() {
			if mm, ok := m.(interface{ ToEntity() E }); ok {
				*entity = mm.ToEntity()
			}
		}()
		return scope.First(m, r.keyWhereString(key), key.Value())
	})
}

func (r *DBRepository[M, E]) Clone() *DBRepository[M, E] {
	var dst DBRepository[M, E]

	copier.Copy(&dst, r)
	return &DBRepository[M, E]{
		db:          r.db,
		tx:          r.tx,
		defaults:    repository.CopyFrom(r.defaults),
		filterOps:   repository.CopyFrom(r.filterOps),
		validKeys:   repository.CopyFrom(r.validKeys),
		relations:   repository.CopyFrom(r.relations),
		extendsIds:  repository.CopyFrom(r.extendsIds),
		sorts:       repository.CopyFrom(r.sorts),
		extendSorts: repository.CopyFrom(r.extendSorts),
		inTrans:     repository.CopyFrom(r.inTrans),
	}
}

func (r *DBRepository[M, E]) Begin() (*DBRepository[M, E], error) {
	clone := r.Clone()
	clone.tx = clone.db.Begin()
	clone.inTrans = true
	return clone, nil
}

func (r *DBRepository[M, E]) Commit() {
	r.tx.Commit()
	r.inTrans = false
	r.tx = nil
}

func (r *DBRepository[M, E]) Rollback() {
	r.tx.Rollback()
	r.inTrans = false
	r.tx = nil
}

func (r *DBRepository[M, E]) SetNamingStrategy(namer schema.Namer) *DBRepository[M, E] {
	r.namer = namer
	return r
}

func (r *DBRepository[M, E]) getScope() *gorm.DB {
	if r.inTrans {
		return r.tx
	} else {
		return r.db
	}
}

func (r *DBRepository[M, E]) getSchema() (*schema.Schema, error) {
	var m M
	if r.schema == nil {
		sch, err := schema.Parse(m, &sync.Map{}, r.namer)
		if err != nil {
			return nil, err
		}
		r.schema = sch
	}

	return r.schema, nil
}

func (r *DBRepository[M, E]) Field(name string) (*schema.Field, error) {
	sch, err := r.getSchema()
	if err != nil {
		return nil, err
	}

	extname := utils.CamelCase(name)

	if _, ok := r.extendsIds[extname]; ok {
		return &schema.Field{
			Name:   utils.CamelCase(name),
			DBName: extname,
		}, nil
	}

	if field := sch.LookUpField(name); field == nil {
		return nil, fmt.Errorf("invalid field id %s", name)
	} else {
		return field, nil
	}
}

func (r *DBRepository[M, E]) FieldName(name string) (string, error) {
	if field, err := r.Field(name); err != nil {
		return "", err
	} else {
		return field.Name, nil
	}
}

func (r *DBRepository[M, E]) DBName(name string) (string, error) {
	if field, err := r.Field(name); err != nil {
		return "", err
	} else {
		return field.DBName, nil
	}
}

func (r *DBRepository[M, E]) Find(ctx context.Context, opts ...repository.SearchOptFunc) ([]E, repository.SearchMetadata, error) {
	var (
		scope    = r.getScope().WithContext(ctx)
		models   = make([]M, 0, 100)
		metadata repository.SearchMetadata
	)

	sch, err := r.getSchema()
	if err != nil {
		return nil, metadata, err
	}

	so, err := r.getSearchOptions("Find", opts, sch)
	if err != nil {
		return nil, metadata, err
	}

	if so.Scope != nil {
		scope = so.Scope
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
	if so.Page.Page > 0 && so.Page.PageSize > 0 {
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
		typeop, ok := r.filterOps[field.Name]
		if !ok {
			return nil, fmt.Errorf("no register filter id %s", filter.ID)
		}

		if bindOp, ok := typeop.Oper.(ScopeWrap); ok {
			filter.Type = typeop.Type
			scope = bindOp.WithScope(scope, func() {
				typeop.Oper.Op(field.DBName, filter.Val())
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
		if custom, ok := r.extendSorts[utils.CamelCase(sort.Field)]; ok {
			return custom(scope, sort.Field, sort.Direction)
		}
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

	scope = r.applySearchColumns(so, scope)

	_ = metadata
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

	return SliceGo2Pb[M, E](models), metadata, nil
}

func (r *DBRepository[M, E]) withDebug(ctx context.Context, scope Scope, fn func(scope Scope) Scope) error {
	if _, ok := ctx.Value(debugSQLKey).(bool); ok {
		sql := scope.ToSQL(func(tx *gorm.DB) *gorm.DB {
			return fn(tx)
		})
		log.Printf("DEBUG SQL: %s", sql)
		return nil
	} else {
		return fn(scope).Error
	}
}

func (r *DBRepository[M, E]) Insert(ctx context.Context, entity *E, ops ...repository.PutOptFunc) error {
	var (
		scope = r.getScope()
		g     M
		m     = g.FromEntity(*entity)
	)
	opts, err := r.buildPutOpts(ops)
	if err != nil {
		return err
	}

	if opts.Scope != nil {
		scope = opts.Scope
	}

	scope = r.applyOnConflict(scope, opts)

	scope = r.applyUpdateColumns(scope, opts)

	if len(opts.LoadKeys) > 0 {
		newScope := scope.Session(&gorm.Session{})
		var attrs = make(map[string]interface{})
		for key, fn := range opts.LoadKeys {
			attrs[key] = fn(m)
		}

		if err := r.withDebug(ctx, newScope, func(scope Scope) Scope {
			return scope.Session(&gorm.Session{}).First(m, attrs)
		}); err != nil {
			return err
		}
	}

	defer func() {
		if mm, ok := m.(interface{ ToEntity() E }); ok {
			*entity = mm.ToEntity()
		}
	}()

	// 扩展 db scopes 处理
	for _, dbScope := range opts.DBScopes {
		scope = dbScope(scope)
	}

	return r.withDebug(ctx, scope, func(tx Scope) Scope {
		if opts.ForceCreate {
			return tx.Model(m).Create(m)
		}
		return tx.Model(m).Save(m)
	})
}

func (r *DBRepository[M, E]) applyOnConflict(scope Scope, opts *repository.PutOption) Scope {
	if len(opts.ConflictColumns) > 0 {
		var (
			columns []clause.Column
		)

		for _, col := range opts.ConflictColumns {
			columns = append(columns, clause.Column{Name: col})
		}

		scope = scope.Clauses(clause.OnConflict{
			Columns:   columns,
			DoUpdates: opts.Assignment,
		})
	}

	return scope
}

func (r *DBRepository[M, E]) applyRelations(scope Scope, relations []repository.RelationItem) Scope {
	return Chain(relations, scope, func(item repository.RelationItem, scope Scope) Scope {
		return scope.Preload(item.Association, item.Args...)
	})
}

func (r *DBRepository[M, E]) applyUpdateColumns(scope Scope, opts *repository.PutOption) Scope {
	if len(opts.Select) > 0 {
		if len(opts.Select) > 1 {
			scope = scope.Select(opts.Select[0], slice.Map(opts.Select[1:], func(s string) interface{} { return s })...)
		} else {
			scope = scope.Select(opts.Select[0])
		}
	}

	if len(opts.Omit) > 0 {
		scope = scope.Omit(opts.Omit...)
	}
	return scope
}

func (r *DBRepository[M, E]) applyTotal(ctx context.Context, scope Scope) Scope {
	var totals = &mysqlSelectTotals{tx: scope, Debug: IsDebug(ctx)}
	scope = scope.WithContext(context.WithValue(scope.Statement.Context, selectTotalKey, totals))
	return scope.Clauses(totals)
	// return scope
}

func (r *DBRepository[M, E]) applySearchColumns(opts *repository.SearchOpt, scope Scope) Scope {
	if len(opts.Select) > 0 {
		if len(opts.Select) > 1 {
			scope = scope.Select(opts.Select[0], slice.Map(opts.Select[1:], func(s string) interface{} { return s })...)
		} else {
			scope = scope.Select(opts.Select[0])
		}
	}

	if len(opts.Omit) > 0 {
		scope = scope.Omit(opts.Omit...)
	}
	return scope
}
func (r *DBRepository[M, E]) applyMetadata(metadata *repository.SearchMetadata, models []M) {
	if len(models) > 0 {
		var (
			first = models[0]
			last  = models[len(models)-1]
		)

		metadata.Count = len(models)

		if pval, err := r.getPrimaryValue(first); err == nil {
			metadata.PrevID = convert.ToStr(pval)
		}

		if pval, err := r.getPrimaryValue(last); err == nil {
			metadata.NextID = convert.ToStr(pval)
		}
		// matedata.Total = r.Count(ctx, specification)
	}

}

func (r *DBRepository[M, E]) getPrimaryValue(m any) (interface{}, error) {
	return GetPrimaryValue(m)
}

func (r *DBRepository[M, E]) Delete(ctx context.Context, entity E) error {
	var (
		scope = r.getScope()
		g     M
		m     = g.FromEntity(entity)
	)

	return r.withDebug(ctx, scope, func(tx Scope) Scope {
		return tx.Model(m).Delete(m)
	})
}

func (r *DBRepository[M, E]) buildPutOpts(ops []PutOptFunc) (*PutOption, error) {
	var opts PutOption
	for _, op := range ops {
		if err := op(&opts); err != nil {
			return nil, err
		}
	}

	return &opts, nil
}

func (r *DBRepository[M, E]) returnKeysExpr(models []M, key string) clause.Expression {
	vals := slice.Map(models, func(m M) interface{} {
		v := reflect.ValueOf(m)
		v = reflect.Indirect(v)

		return v.FieldByName(key).Interface()
	})
	return clause.And(
		clause.IN{
			Column: utils.SnakeCase(key),
			Values: vals,
		},
	)
}

func (r *DBRepository[M, E]) getSearchOptions(method string, opts []SearchOptFunc, sch *schema.Schema) (*SearchOpt, error) {
	searchOpts, ok := r.defaults[method]
	if !ok {
		searchOpts = repository.DefaultSearchOpt
	}

	searchOpts.AddValidKey(r.validKeys...)
	searchOpts.SetSchema(sch)
	for _, seop := range opts {
		if err := seop(&searchOpts); err != nil {
			return nil, err
		}
	}

	return &searchOpts, nil
}

func (r *DBRepository[M, E]) DefaultsOpts(method string, opts SearchOpt) *DBRepository[M, E] {
	r.defaults[method] = opts
	return r
}

func (r *DBRepository[M, E]) SetValidKey(key ...string) *DBRepository[M, E] {
	for _, k := range key {
		if !slices.Contains(r.validKeys, k) {
			r.validKeys = append(r.validKeys, k)
		}
	}
	return r
}

func (r *DBRepository[M, E]) AddFilter(id string, op Oper, ftType ...repository.FTType) *DBRepository[M, E] {

	defType := func(ftTypes []repository.FTType) repository.FTType {
		if len(ftTypes) > 0 {
			return ftTypes[0]
		}

		return repository.FTAuto
	}

	r.filterOps[id] = TypeOper{
		Oper: op,
		Type: defType(ftType),
	}

	return r
}

func (r *DBRepository[M, E]) AddCustomFilter(id string, fn CustomOpFunc) *DBRepository[M, E] {
	r.extendsIds[utils.CamelCase(id)] = true
	r.filterOps[id] = TypeOper{
		Oper: CustomFilter(fn),
	}

	return r
}

func (r *DBRepository[M, E]) AddSort(field string, defOrder ...OrderDirection) *DBRepository[M, E] {
	if len(defOrder) == 0 {
		r.sorts[field] = repository.OrderAuto
	} else {
		r.sorts[field] = defOrder[0]
	}
	return r
}

func (r *DBRepository[M, E]) AddCustomSort(field string, fn CustomSortFunc, defOrder ...OrderDirection) *DBRepository[M, E] {
	r.extendsIds[utils.CamelCase(field)] = true

	if len(defOrder) == 0 {
		r.sorts[field] = repository.OrderAuto
	} else {
		r.sorts[field] = defOrder[0]
	}

	r.extendSorts[utils.CamelCase(field)] = fn
	return r
}

func (r *DBRepository[M, E]) AddRelation(association string, refDef repository.RelationDef) *DBRepository[M, E] {
	r.relations[association] = refDef
	return r
}

func (r *DBRepository[M, E]) AddRelationJoin(association string, query ...string) *DBRepository[M, E] {
	if len(query) > 0 {
		var (
			q         = query[0]
			argsCount = parseCount(q)
		)
		r.AddRelation(association, repository.RelationDef{
			Join:      true,
			Query:     q,
			ArgsCount: argsCount,
		})
	} else {
		r.AddRelation(association, repository.RelationDef{
			Join: true,
		})
	}

	return r
}

func (r *DBRepository[M, E]) AddRelationPreload(association string, query ...string) *DBRepository[M, E] {
	if len(query) > 0 {
		var (
			q         = query[0]
			argsCount = parseCount(q)
		)
		r.AddRelation(association, repository.RelationDef{
			Query:     q,
			ArgsCount: argsCount,
		})
	} else {
		r.AddRelation(association, repository.RelationDef{})
	}

	return r
}

func (r *DBRepository[M, E]) defaultSorts(sorts []repository.SortMode) []repository.SortMode {
	var (
		newSorts []repository.SortMode
		sortKeys = slice.Map(sorts, func(sm repository.SortMode) string {
			return sm.Field
		})
	)

	newSorts = append(newSorts, sorts...)
	for key, order := range r.sorts {
		if order == repository.OrderAuto {
			continue
		}

		if !slices.Contains(sortKeys, key) {
			newSorts = append(newSorts, repository.SortMode{
				Field:     key,
				Direction: order,
			})
		}
	}
	return newSorts
}

func (r *DBRepository[M, E]) validSorts(sorts []repository.SortMode) error {
	var errs error
	for _, sort := range sorts {
		field, err := r.Field(sort.Field)
		if err != nil {
			errs = multierr.Append(errs, fmt.Errorf("invalid sort field %s because %w", sort.Field, err))
		}

		if _, ok := r.sorts[field.Name]; !ok {
			errs = multierr.Append(errs, fmt.Errorf("no register sort field %s", sort.Field))
		}
	}
	return errs
}

func (r *DBRepository[M, E]) validFilters(filters []repository.FilterItem) error {
	var errs error

	for _, filter := range filters {
		if ok := r.extendsIds[utils.CamelCase(filter.ID)]; ok {
			continue
		}
		id := utils.SnakeCase(filter.ID)
		field, err := r.Field(id)
		if err != nil {
			errs = multierr.Append(errs, fmt.Errorf("invalid filter field %s because %w", filter.ID, err))
			continue
		}

		if _, ok := r.filterOps[field.Name]; !ok {
			errs = multierr.Append(errs, fmt.Errorf("no register filter ID %s", filter.ID))
		}
	}
	return errs
}

func Chain[T any](list []T, scope Scope, fn func(e T, scope Scope) Scope) Scope {
	for _, e := range list {
		scope = fn(e, scope)
	}
	return scope
}

func ChainErr[T any](list []T, scope Scope, fn func(e T, scope Scope) (Scope, error)) (Scope, error) {
	var (
		errs error
		err  error
	)
	for _, e := range list {
		scope, err = fn(e, scope)
		if err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return scope, errs
}

func parseCount(q string) int {
	return strings.Count(q, "?")
}

func GetPrimaryValue(m any) (interface{}, error) {
	var v = reflect.ValueOf(m)
	v = reflect.Indirect(v)
	sch, err := schema.Parse(m, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		return "", err
	}

	if len(sch.PrimaryFields) == 0 {
		return "", errors.New("do not have primary key")

	}
	pfield := sch.PrimaryFields[0]
	return v.FieldByName(pfield.Name).Interface(), nil
}
