package globalkey

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/tj/assert"
)

func TestSetInc(t *testing.T) {
	var (
		db, mock = redismock.NewClientMock()
	)

	set := NewCounterRecord[uint, int]("set_test$$", db)
	assert.NotNil(t, set)

	mock.ExpectIncr("set_test$$:1234").SetVal(1)

	v, err := set.Inc(1234)
	assert.NoError(t, err)
	assert.Equal(t, v, 1)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestSetPattern(t *testing.T) {
	var (
		db, mock = redismock.NewClientMock()
	)

	set := NewCounterRecord[uint, int]("set_test$$", db, OptSetPattern(func(prefix string, key any) string {
		return fmt.Sprintf("%s:%s:%v", prefix, time.Now().Format("2006-01-02"), key)
	}))
	assert.NotNil(t, set)

	mock.ExpectIncr(fmt.Sprintf("set_test$$:%s:1234", time.Now().Format("2006-01-02"))).SetVal(1)

	v, err := set.Inc(1234)
	assert.NoError(t, err)
	assert.Equal(t, v, 1)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestSetDec(t *testing.T) {
	var (
		db, mock = redismock.NewClientMock()
	)

	set := NewCounterRecord[uint, int]("set_test$$", db)
	assert.NotNil(t, set)

	mock.ExpectDecr("set_test$$:1234").SetVal(-1)

	v, err := set.Dec(1234)
	assert.NoError(t, err)
	assert.Equal(t, v, -1)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestSetStore(t *testing.T) {
	var (
		db, mock = redismock.NewClientMock()
	)

	set := NewCounterRecord[uint, int]("set_test$$", db)
	assert.NotNil(t, set)

	mock.ExpectSetEX("set_test$$:1234", 3, 0).RedisNil()
	ok := set.Store(1234, 3)

	assert.False(t, ok)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
