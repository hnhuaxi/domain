package domain

import (
	"errors"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

var (
	ErrInvalidDriverType = errors.New("invalid driver type")
	ErrCantPrimaryKey    = errors.New("can't get primary key")
	ErrMustNotZero       = errors.New("must not zero value")
	ErrNotFound          = errors.New("not found")
)

func CheckDuplicate(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return true
	}

	return false
}

func CheckDuplicateRelation(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1452 {
		return true
	}

	return false
}

func CheckNotFound(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return true
	}

	// 不知道为什么返回一个未找到，不是 ErrRecordNotFound
	// 答：可能是依赖库的版本问题，之前使用的是 github.com/jinzhu/gorm
	return err.Error() == "record not found"
}
