package db

import (
	"context"
	"testing"

	"github.com/hnhuaxi/domain/repository"
	"github.com/hnhuaxi/platform/logger"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Account struct {
	ID     uint
	UserID uint
	User   User
	Total  float64
}

type PAccount struct {
	Id     uint32
	UserId uint32
	User   *PUser
	Total  float64
}

func (account *Account) ToEntity() *PAccount {
	return &PAccount{
		Id:     uint32(account.ID),
		UserId: uint32(account.UserID),
		Total:  account.Total,
	}
}

func (account *Account) FromEntity(entity *PAccount) interface{} {
	return &Account{
		ID:     uint(entity.Id),
		UserID: uint(entity.UserId),
		Total:  entity.Total,
	}
}

func TestNewDbRelationRepository(t *testing.T) {
	var (
		db  = testDB()
		r   = NewDBRelationRepository[*User, *PUser, *Account, *PAccount](db, &logger.Logger{}, "Accounts")
		usr = PUser{
			Id:   7,
			Name: "bob",
		}
		ctx = context.Background()
	)

	r.SetValidKey("ID")

	r.AddFilter("Total", EQ) // Define Filter Name Equals
	// r.AddFilter("Age", GT)  // Define Filter Age Great
	r.AddSort("Total")

	// r.AddSort("Age", OrderAsc)
	// r.AddRelationPreload("Team")
	// r.AddSort()
	_, _, err := r.Find(WithDebugSQL(ctx), &usr,
		repository.OptOffset(200),
		repository.OptPageSize(100),
		// OptDBScope(func(tx Scope) Scope {
		// 	return tx.Where("age > 18")
		// }),
		repository.OptAfter(repository.ID(10)),
		repository.OptFilter(
			repository.FilterItem{
				ID:    "Total",
				Value: 10.3,
			},
		),
	)
	assert.NoError(t, err)
}

func TestDbRelationRepositoryAppend(t *testing.T) {

}

func TestJoinClause(t *testing.T) {
	var (
		db    = testDB()
		users []*User
	)

	scope := db.Clauses(clause.Join{
		Type: clause.InnerJoin,
		Table: clause.Table{
			Name: "user_accounts",
		},
		ON:         clause.Where{},
		Using:      []string{},
		Expression: nil,
	})

	sql := scope.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Find(&users)
	})
	t.Logf("SQL: %s", sql)
}

func TestJoinAssociation(t *testing.T) {
	var (
		db       = testDB()
		accounts []*Account
	)

	joinExprs, err := JoinBuilder(&User{
		ID: 1,
	}, &Account{}, "Accounts")
	assert.NoError(t, err)

	scope := db.Clauses(joinExprs...)

	sql := scope.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Find(&accounts)
	})
	t.Logf("SQL: %s", sql)
}
