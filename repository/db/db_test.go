package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/hnhuaxi/domain/repository"
	"github.com/hnhuaxi/platform/logger"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func testDB() *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	// db.AutoMigrate(&model.User{})
	return db
}

var db *gorm.DB

func TestMain(m *testing.M) {
	db = testDB()
	m.Run()
}

type User struct {
	ID        uint `gorm:"primaryKey"`
	Name      string
	Age       int
	ManagerID *uint
	Accounts  []Account `gorm:"many2many:user_accounts;"`
	Team      []User    `gorm:"foreignkey:ManagerID"`
}

type PUser struct {
	Id   uint
	Name string
	Age  uint32
}

func (u *User) ToEntity() *PUser {
	return &PUser{
		Id:   u.ID,
		Name: u.Name,
		Age:  uint32(u.Age),
	}
}

func (u *User) FromEntity(entity *PUser) interface{} {
	return &User{
		ID:   entity.Id,
		Name: entity.Name,
		Age:  int(entity.Age),
	}
}

func TestImplRepository(t *testing.T) {
	var iface repository.Repository[*User, *PUser] = NewDBRepository[*User, *PUser](db, &logger.Logger{})
	_ = iface
}

func TestNewDbRepository(t *testing.T) {
	var db = testDB()
	r := NewDBRepository[*User, *PUser](db, &logger.Logger{})
	assert.NotNil(t, r)
	var ctx = context.Background()
	r.SetValidKey("ID").
		AddFilter("Name", EQ). // Define Filter Name Equals
		AddFilter("Age", GT).  // Define Filter Age Great
		AddSort("Name").
		AddSort("Age", repository.OrderAsc).
		AddRelationPreload("Team").
		AddCustomFilter("Extend", func(tx Scope, key, val interface{}) Scope {
			return tx.Where("moeny > ?", val)
		})
	r.AddCustomSort("Money", func(scope Scope, field string, direction repository.OrderDirection) Scope {
		return scope.Order(fmt.Sprintf("money %s", direction))
	})
	// r.AddSort()
	users, _, err := r.Find(WithDebugSQL(ctx),
		repository.OptOffset(200),
		repository.OptPageSize(100),
		// OptDBScope(func(tx Scope) Scope {
		// 	return tx.Where("age > 18")
		// }),
		repository.OptAfter(repository.ID(10)),
		repository.OptFilter(repository.FilterItem{
			ID:    "Name",
			Value: "bob",
		}, repository.FilterItem{
			ID:    "age",
			Value: 25,
		}, repository.FilterItem{
			ID:    "Extend",
			Value: 10000,
		}),
		repository.OptSort(
			repository.SortMode{
				Field:     "Name",
				Direction: repository.OrderDesc,
			},
			repository.SortMode{
				Field:     "Age",
				Direction: repository.OrderDesc,
			},
			repository.SortMode{
				Field:     "Money",
				Direction: repository.OrderAsc,
			},
		),
		repository.OptField(
			repository.FieldItem{
				Name: "ID",
			},
			repository.FieldItem{
				Name: "Name",
			},
		),
		repository.OptRelationItem(repository.RelationItem{
			Association: "Team",
		}),
	)
	assert.NoError(t, err)
	_ = users
}

func TestDBRepositoryPut(t *testing.T) {
	var (
		db   = testDB()
		ctx  = context.Background()
		user = &PUser{
			Id:   0,
			Name: "",
			Age:  0,
		}
	)
	r := NewDBRepository[*User, *PUser](db, &logger.Logger{})
	assert.NotNil(t, r)
	// auto create
	err := r.Insert(WithDebugSQL(ctx), &user)
	assert.NoError(t, err)

	user = &PUser{
		Id:   1,
		Name: "",
		Age:  0,
	}
	// auto update
	err = r.Insert(WithDebugSQL(ctx), &user)
	assert.NoError(t, err)
	// anothr primary key
	user = &PUser{
		Id:   0,
		Name: "hello",
		Age:  0,
	}
	err = r.Insert(WithDebugSQL(ctx), &user, OptOverwrite("Name"))
	assert.NoError(t, err)
}

func TestDBRepositoryGet(t *testing.T) {
	var (
		db   = testDB()
		ctx  = context.Background()
		user = &PUser{
			Id:   0,
			Name: "",
			Age:  0,
		}
	)
	r := NewDBRepository[*User, *PUser](db, &logger.Logger{})
	assert.NotNil(t, r)
	puser, err := r.Get(WithDebugSQL(ctx), repository.ID(1))
	assert.NoError(t, err)
	assert.NotNil(t, puser)

	err = r.GetInto(WithDebugSQL(ctx), &user, repository.ID(1))
	assert.NoError(t, err)
}
