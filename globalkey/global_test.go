package globalkey

import (
	"testing"

	"github.com/go-redis/redismock/v8"
	"github.com/tj/assert"
)

func TestNewGlobalKey(t *testing.T) {
	var (
		db, _ = redismock.NewClientMock()
	)

	type User struct {
		ID   uint
		Name string
	}

	key := NewGlobalKey[*User]("test$$struct", db)

	u, ok := key.Load()
	assert.False(t, ok)
	assert.Nil(t, u)
}
