package singleton

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestUser struct {
	ID       uint
	Username string
	Age      int
}

func TestNew(t *testing.T) {
	var usr = New(func() *TestUser {
		return &TestUser{
			ID:       10,
			Username: "bob",
			Age:      18,
		}
	})
	t.Logf("usr %v", usr)
	usr1 := New(func() *TestUser {
		return &TestUser{
			ID:       10,
			Username: "bob",
			Age:      18,
		}
	})

	assert.Equal(t, usr1, usr)
}
