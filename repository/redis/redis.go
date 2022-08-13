package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/hnhuaxi/domain"
	"github.com/hnhuaxi/domain/repository"
	"github.com/hnhuaxi/domain/repository/db"
	"github.com/hnhuaxi/domain/utils"

	"github.com/hnhuaxi/platform/logger"
	"github.com/hysios/utils/convert"
	"go.uber.org/multierr"
	"gorm.io/gorm/schema"
)

type RedisRepository[M repository.Model[E], E any] struct {
	NS string

	logger    *logger.Logger
	redis     *redis.Client
	defaults  map[string]repository.SearchOpt
	schema    *schema.Schema
	validKeys []string
}

func NewRedisRepository[M repository.Model[E], E any](namespace string, redis *redis.Client, logger *logger.Logger) *RedisRepository[M, E] {
	return &RedisRepository[M, E]{
		NS:       namespace,
		logger:   logger,
		redis:    redis,
		defaults: make(map[string]repository.SearchOpt),
	}
}

func (rredis *RedisRepository[M, E]) getKey(key string) string {
	return rredis.NS + "$$" + key
}

type anykey struct {
	name string
	val  reflect.Value
}

func (key *anykey) Name() string {
	return key.name
}

func (key *anykey) Value() interface{} {
	return key.val.Interface()
}

func (rredis *RedisRepository[M, E]) getModelId(val any) (repository.Key, bool) {
	v := reflect.ValueOf(val)

	for v.Kind() == reflect.Ptr {
		v = reflect.Indirect(v)
	}

	var ids = []string{"ID", "Id"}

	for _, id := range ids {
		fe := v.FieldByName(id)
		if fe.IsValid() {
			return &anykey{name: id, val: fe}, true
		}
	}

	return nil, false
}

func (rredis *RedisRepository[M, E]) getModel(val any) string {
	t := reflect.TypeOf(val)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return utils.SnakeCase(t.Name())
}

func (rredis *RedisRepository[M, E]) fullkey(key repository.Key) string {
	var (
		m M
	)

	modelName := rredis.getModel(m)
	return rredis.getKey(fmt.Sprintf("%s:%v", modelName, key.Value()))
}

func (rredis *RedisRepository[M, E]) Insert(ctx context.Context, entity E, ops ...repository.PutOptFunc) error {
	var (
		m    = repository.FromEntity[M](entity)
		opts repository.PutOption
	)
	key, ok := rredis.getModelId(m)
	if !ok {
		return domain.ErrCantPrimaryKey
	}

	for _, op := range ops {
		if err := op(&opts); err != nil {
			return err
		}
	}

	v := reflect.ValueOf(key.Value())
	if v.IsZero() {
		return domain.ErrMustNotZero
	}
	modelName := rredis.getModel(m)

	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	if err := rredis.redis.Set(ctx, rredis.getKey(fmt.Sprintf("%s:%v", modelName, key.Value())), b, opts.Expires).Err(); err != nil {
		return err
	}

	return nil
}

func (rredis *RedisRepository[M, E]) Get(ctx context.Context, key repository.Key, ops ...repository.SearchOptFunc) (E, error) {
	var (
		opts repository.SearchOpt
		z    E
		m    = rredis.instantM()
	)

	for _, op := range ops {
		if err := op(&opts); err != nil {
			return z, err
		}
	}

	b, err := rredis.redis.GetEx(ctx, rredis.fullkey(key), opts.Expiration).Result()
	if err != nil {
		return z, err
	}

	if err := json.Unmarshal([]byte(b), m); err != nil {
		return z, err
	}

	return m.ToEntity(), nil
}

func (rredis *RedisRepository[M, E]) getSchema() (*schema.Schema, error) {
	var m M
	if rredis.schema == nil {
		sch, err := schema.Parse(m, &sync.Map{}, schema.NamingStrategy{})
		if err != nil {
			return nil, err
		}
		rredis.schema = sch
	}

	return rredis.schema, nil
}

func (rredis *RedisRepository[M, E]) Find(ctx context.Context, ops ...repository.SearchOptFunc) ([]E, repository.SearchMetadata, error) {
	var (
		metadata repository.SearchMetadata
		cursor   uint64
		m        M
		cmds     []*redis.StringCmd
		results  []M
		match    = rredis.getKey(rredis.getModel(m) + ":*")
	)

	sch, err := rredis.getSchema()
	if err != nil {
		return nil, metadata, err
	}

	opts, err := rredis.getSearchOptions("Find", ops, sch)
	if err != nil {
		return nil, metadata, err
	}

	if opts.Page.AfterId != nil {
		cursor, _ = convert.Uint64(opts.Page.AfterId.Value())
	}

	keys, cursor, err := rredis.redis.Scan(ctx, cursor, match, int64(opts.Page.PageSize)).Result()
	if err != nil {
		return nil, metadata, err
	}

	pipe := rredis.redis.Pipeline()

	for _, key := range keys {
		cmds = append(cmds, pipe.GetEx(ctx, key, opts.Expiration))
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, metadata, err
	}

	// var
	var (
		errs error
	)
	for _, cmd := range cmds {
		m := rredis.instantM()

		if err := json.Unmarshal([]byte(cmd.Val()), m); err != nil {
			errs = multierr.Append(errs, err)
		}
		results = append(results, m)
	}

	metadata.NextID = strconv.Itoa(int(cursor))

	return db.SliceGo2Pb[M, E](results), metadata, errs
}

func (rredis *RedisRepository[M, E]) Delete(ctx context.Context, entity E) error {
	var (
		m = repository.FromEntity[M](entity)
	)

	key, ok := rredis.getModelId(m)
	if !ok {
		return domain.ErrCantPrimaryKey
	}

	_, err := rredis.redis.Del(ctx, rredis.fullkey(key)).Result()
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisRepository[M, E]) getSearchOptions(method string, opts []repository.SearchOptFunc, sch *schema.Schema) (*repository.SearchOpt, error) {
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

func (r *RedisRepository[M, E]) instantE() E {
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

func (r *RedisRepository[M, E]) instantM() M {
	var (
		m M
		v = reflect.ValueOf(m)
		t = reflect.TypeOf(m)
	)

	if t.Kind() == reflect.Ptr {
		instance := reflect.New(t.Elem())
		m, _ = instance.Interface().(M)
		return m
	} else if v.IsValid() && !v.IsNil() {
		return m
	} else {
		panic("invalid model type")
	}
}
