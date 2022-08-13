package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/hnhuaxi/domain/repository"
	"github.com/hnhuaxi/platform/logger"
)

type TestUser struct {
	ID   uint
	Name string
	Age  int
}

type PTestUser struct {
	Id   uint32
	Name string
	Age  uint32
}

func (u *TestUser) ToEntity() *PTestUser {
	return &PTestUser{
		Id:   uint32(u.ID),
		Name: u.Name,
		Age:  uint32(u.Age),
	}
}

func (u *TestUser) FromEntity(entity *PTestUser) interface{} {
	return &TestUser{
		ID:   uint(entity.Id),
		Name: entity.Name,
		Age:  int(entity.Age),
	}
}

func TestRedisInsert(t *testing.T) {
	var (
		ctx      = context.Background()
		db, mock = redismock.NewClientMock()
		redis    = NewRedisRepository[*TestUser, *PTestUser]("tests", db, logger.ProviderLog())
		u        = &PTestUser{Id: 10, Name: "bob", Age: 18}
		m        = (*TestUser)(nil).FromEntity(u)
	)
	err := redis.Insert(ctx, u, repository.OptExpires(10*time.Second))
	t.Logf("err %v", err)
	// assert.NoError(t, err)
	mock.ExpectSet("tests$$test_user:10", jsonify(m), 10*time.Second).RedisNil()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func jsonify(val any) []byte {
	b, _ := json.Marshal(val)
	return b
}

func TestRedisGet(t *testing.T) {
	var (
		ctx      = context.Background()
		db, mock = redismock.NewClientMock()
		redis    = NewRedisRepository[*TestUser, *PTestUser]("tests", db, logger.ProviderLog())
	)
	_, err := redis.Get(ctx, repository.ID(10), repository.OptExpiration(10*time.Second))
	t.Logf("err %v", err)
	// assert.NoError(t, err)
	mock.Regexp().ExpectGet("test_user:10")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
