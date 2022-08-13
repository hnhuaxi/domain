package db

import (
	"fmt"

	"github.com/hnhuaxi/domain/repository"
	"github.com/hysios/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var selectTotalKey = struct{}{}

type mysqlSelectTotals struct {
	tx    *gorm.DB
	Debug bool
}

func (*mysqlSelectTotals) ModifyStatement(stmt *gorm.Statement) {
	stmt.Selects = append([]string{"SQL_CALC_FOUND_ROWS *"}, stmt.Selects...)
}

func (*mysqlSelectTotals) Build(builder clause.Builder) {
	log.Infof("builder %v", builder)
}

func (totals *mysqlSelectTotals) Total(val any) error {
	return DebugSQL(totals.Debug, totals.tx, func(scope repository.Scope) repository.Scope {
		return totals.tx.Raw("SELECT FOUND_ROWS()").Scan(val)
	})
}

func DebugSQL(debug bool, scope repository.Scope, fn func(scope repository.Scope) repository.Scope) error {
	if debug {
		sql := scope.ToSQL(func(tx *gorm.DB) *gorm.DB {
			scope = fn(tx)

			return scope
		})
		fmt.Printf("SQL: %s", sql)
		return nil
	} else {
		scope = fn(scope)
	}

	return scope.Error
}

type GetTotal interface {
	Total(any) error
}

func GetScopeTotal(scope repository.Scope) (GetTotal, bool) {
	if totals, ok := scope.Statement.Context.Value(selectTotalKey).(GetTotal); ok {
		return totals, ok
	}

	return nil, false
}
