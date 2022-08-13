package repository

import (
	"fmt"
	"time"

	"github.com/imdario/mergo"
	"golang.org/x/exp/slices"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type SearchOpt struct {
	Page       PageOption
	Filters    []FilterItem
	Sorts      []SortMode
	Fields     []FieldItem
	Relations  []RelationItem
	DBScopes   []ScopeFunc
	Select     []string
	Omit       []string
	Expiration time.Duration
	Customs    CustomOption

	validKeys []string
	sch       *schema.Schema
}

type PutOption struct {
	ConflictColumns     []string
	Assignment          clause.Set
	SkipAllAssociations bool
	SkipAssociations    []string
	Select              []string // 先择字段
	Omit                []string // 忽略字段
	ReturnKey           string
	ReturnColumns       []string
	LoadKeys            map[string]KeyFunc
	ForceCreate         bool
	Expires             time.Duration
}

type CustomOption struct {
	options map[string]interface{}
}

type KeyFunc func(model interface{}) interface{}

type PutOptFunc func(opt *PutOption) error

type PageOption struct {
	AfterId    Key
	BeforeId   Key
	PageSize   int
	PageOffset int
}

type SearchOptFunc func(*SearchOpt) error

var DefaultSearchOpt = SearchOpt{
	Page: PageOption{
		PageSize: 20,
	},
}

func (so *SearchOpt) ValidateKey(key Key) error {
	fName := so.FieldName(key.Name())
	if fName != "ID" && !slices.Contains(so.validKeys, fName) {
		return fmt.Errorf("invalid key '%s'", key.Name())
	}
	return nil
}

func (so *SearchOpt) AddValidKey(keys ...string) {
	so.validKeys = append(so.validKeys, keys...)
}

func (so *SearchOpt) SetSchema(sch *schema.Schema) {
	so.sch = sch
}

func (so *SearchOpt) FieldName(name string) string {
	if so.sch == nil {
		return name
	}

	field := so.sch.LookUpField(name)
	if field == nil {
		panic(fmt.Sprintf("not found field %s", name))
	}

	return field.Name
}

func OptAfter(key Key) SearchOptFunc {
	return func(so *SearchOpt) error {
		if err := so.ValidateKey(key); err != nil {
			return err
		}
		so.Page.AfterId = key
		return nil
	}
}

func OptBefore(key Key) SearchOptFunc {
	return func(so *SearchOpt) error {
		if err := so.ValidateKey(key); err != nil {
			return err
		}
		so.Page.BeforeId = key
		return nil
	}
}

func OptPageSize(size int) SearchOptFunc {
	return func(so *SearchOpt) error {
		so.Page.PageSize = size
		return nil
	}
}

func OptOffset(oft int) SearchOptFunc {
	return func(so *SearchOpt) error {
		so.Page.PageOffset = oft
		return nil
	}
}

func OptDBScope(fn ScopeFunc) SearchOptFunc {
	return func(so *SearchOpt) error {
		so.DBScopes = append(so.DBScopes, fn)
		return nil
	}
}

func OptField(item ...FieldItem) SearchOptFunc {
	return func(so *SearchOpt) error {
		so.Fields = append(so.Fields, item...)
		return nil
	}
}

func OptFilter(item ...FilterItem) SearchOptFunc {
	return func(so *SearchOpt) error {
		so.Filters = append(so.Filters, item...)
		return nil
	}
}

func OptSort(item ...SortMode) SearchOptFunc {
	return func(so *SearchOpt) error {
		so.Sorts = append(so.Sorts, item...)
		return nil
	}
}

func OptRelationItem(item ...RelationItem) SearchOptFunc {
	return func(so *SearchOpt) error {
		so.Relations = append(so.Relations, item...)
		return nil
	}
}

func OptRelation(association string, args ...interface{}) SearchOptFunc {
	return func(so *SearchOpt) error {
		so.Relations = append(so.Relations, RelationItem{
			Association: association,
			Args:        args,
		})
		return nil
	}
}

func OptMerge(opt *SearchOpt) SearchOptFunc {
	return func(so *SearchOpt) error {
		return mergo.Merge(so, opt, mergo.WithOverride)
		// *so = *opt
	}
}

func OptGetSelect(column ...string) SearchOptFunc {
	return func(so *SearchOpt) error {
		so.Select = column
		return nil
	}
}

func OptGetOmit(column ...string) SearchOptFunc {
	return func(so *SearchOpt) error {
		so.Omit = column
		return nil
	}
}

func OptExpires(duration time.Duration) PutOptFunc {
	return func(opt *PutOption) error {
		opt.Expires = duration
		return nil
	}
}

func OptExpiration(duration time.Duration) SearchOptFunc {
	return func(so *SearchOpt) error {
		so.Expiration = duration
		return nil
	}
}

func (custom *CustomOption) init() {
	if custom.options == nil {
		custom.options = make(map[string]interface{})
	}
}

func (custom *CustomOption) AddOption(key string, value interface{}) {
	custom.init()
	custom.options[key] = value
}

func (custom CustomOption) HasOption(key string) bool {
	custom.init()
	if _, ok := custom.options[key]; ok {
		return true
	}

	return false
}

func (custom CustomOption) Get(key string) (interface{}, bool) {
	custom.init()
	if v, ok := custom.options[key]; ok {
		return v, true
	}

	return nil, false
}

// func OptOnlyCache() repository.SearchOptFunc {
// 	return func(so *repository.SearchOpt) error {
// 		if so.Customs == nil {
// 			so.Customs= make(repository.CustomOption)
// 		}

// 	}
// }
