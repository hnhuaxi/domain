package globalkey

import (
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/tj/assert"
)

func TestCounterKeyInc(t *testing.T) {
	var (
		db, mock = redismock.NewClientMock()
	)

	key := NewCounterKey[int]("counterkey_test", db)
	assert.NotNil(t, key)

	mock.ExpectIncr("counterkey_test").SetVal(1)

	v, err := key.Inc()
	assert.NoError(t, err)
	assert.Equal(t, v, 1)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCounterKeyIncWithExpires(t *testing.T) {
	var (
		db, mock = redismock.NewClientMock()
	)

	key := NewCounterKey[int]("counterkey_test", db, OptExpires(time.Second))
	assert.NotNil(t, key)

	mock.ExpectIncr("counterkey_test").SetVal(1)
	mock.ExpectExpire("counterkey_test", time.Second).SetVal(true)

	v, err := key.Inc()
	assert.NoError(t, err)
	assert.Equal(t, v, 1)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCounterKeyDec(t *testing.T) {
	var (
		// ctx      = context.Background()
		db, mock = redismock.NewClientMock()
	)

	key := NewCounterKey[int]("counterkey_test", db)
	assert.NotNil(t, key)

	mock.ExpectDecr("counterkey_test").SetVal(-1)

	v, err := key.Dec()
	assert.NoError(t, err)
	assert.Equal(t, v, -1)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCounterKeyLoad(t *testing.T) {
	var (
		db, mock = redismock.NewClientMock()
	)

	key := NewCounterKey[int]("counterkey_test", db)
	assert.NotNil(t, key)

	mock.ExpectGet("counterkey_test").SetErr(nil)

	v, ok := key.Load()
	assert.False(t, ok)
	assert.Equal(t, v, 0)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCounterKeyStore(t *testing.T) {
	var (
		db, mock = redismock.NewClientMock()
	)

	key := NewCounterKey[int]("counterkey_test", db)
	assert.NotNil(t, key)

	mock.ExpectSetEX("counterkey_test", 3, 0).RedisNil()
	ok := key.Store(3)

	assert.False(t, ok)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCounterKeyPublish(t *testing.T) {
	var (
		db, mock = redismock.NewClientMock()
	)

	key := NewCounterKey[int]("counterkey_test", db, OptPublish())
	assert.NotNil(t, key)

	mock.ExpectSetEX("counterkey_test", 3, 0).RedisNil()
	mock.ExpectPublish("counterkey_test", []byte{51}).SetErr(nil)
	ok := key.Store(3)

	assert.False(t, ok)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
