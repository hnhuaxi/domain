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
	selectClause := stmt.Clauses["SELECT"]
	if selectClause.AfterExpression != nil {
		stmt.Selects = append([]string{"SQL_CALC_FOUND_ROWS *,"}, stmt.Selects...)
	} else {
		stmt.Selects = append([]string{"SQL_CALC_FOUND_ROWS *"}, stmt.Selects...)
	}
	// selectClause := stmt.Clauses["SELECT"]
	// if selectClause.AfterExpression == nil {
	// 	selectClause.AfterExpression = clause.Select{
	// 		Distinct: false,
	// 		Columns: []clause.Column{
	// 			{
	// 				Name: "SQL_CALC_FOUND_ROWS *",
	// 				Raw:  true,
	// 			},
	// 		},
	// 		Expression: nil,
	// 	}
	// 	stmt.Clauses["SELECT"] = selectClause
	// }
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
