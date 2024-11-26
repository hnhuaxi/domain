package db

import (
	"reflect"

	"github.com/hnhuaxi/domain/repository"
	"gorm.io/gorm/clause"
)

func OptOnConflict(columns []string) PutOptFunc {
	return func(opt *PutOption) error {
		opt.ConflictColumns = columns
		return nil
	}
}

func OptCreate() PutOptFunc {
	return func(opt *PutOption) error {
		opt.ForceCreate = true
		return nil
	}
}

func OptAssignmentColumns(columns []string) PutOptFunc {
	return func(opt *PutOption) error {
		opt.Assignment = clause.AssignmentColumns(columns)
		return nil
	}
}

func OptAssignments(assignments map[string]interface{}) PutOptFunc {
	return func(opt *PutOption) error {
		opt.Assignment = clause.Assignments(assignments)
		return nil
	}
}

func OptSkipAssociations(association ...string) PutOptFunc {
	return func(opt *PutOption) error {
		if len(association) == 0 {
			opt.SkipAllAssociations = true
		} else {
			opt.SkipAssociations = association
		}
		return nil
	}
}

func OptSelect(column ...string) PutOptFunc {
	return func(opt *PutOption) error {
		opt.Select = column
		return nil
	}
}

func OptOmit(column ...string) PutOptFunc {
	return func(opt *PutOption) error {
		opt.Omit = column
		return nil
	}
}

func OptReturnKey(key string, retColumns ...string) PutOptFunc {
	return func(opt *PutOption) error {
		opt.ReturnKey = key
		opt.ReturnColumns = retColumns
		return nil
	}
}

func OptOverwrite(key string, val ...interface{}) PutOptFunc {
	return func(opt *PutOption) error {
		if opt.LoadKeys == nil {
			opt.LoadKeys = make(map[string]KeyFunc)
		}

		if len(val) > 0 {
			opt.LoadKeys[key] = ValueFunc(val[0])
		} else {
			opt.LoadKeys[key] = ValueFromModel(key)
		}
		return nil
	}
}

func OptPutScope(scopefunc repository.ScopeFunc) PutOptFunc {
	return func(opt *PutOption) error {
		opt.DBScopes = append(opt.DBScopes, scopefunc)
		return nil
	}
}

func OptPutDb(scope repository.Scope) PutOptFunc {
	return func(opt *PutOption) error {
		opt.Scope = scope
		return nil
	}
}

func ValueFunc(value interface{}) KeyFunc {
	return func(model interface{}) interface{} {
		return value
	}
}

func ValueFromModel(key string) KeyFunc {
	return func(model interface{}) interface{} {
		v := reflect.ValueOf(model)
		v = reflect.Indirect(v)
		return v.FieldByName(key).Interface()
	}
}
