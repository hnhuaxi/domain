package utils

import (
	"github.com/gertd/go-pluralize"
	"github.com/iancoleman/strcase"
)

var (
	plurClient *pluralize.Client
)

func CamelCase(s string) string {
	return strcase.ToCamel(s)
}

func SnakeCase(s string) string {
	return strcase.ToSnake(s)
}

func Plural(s string) string {
	return plurClient.Plural(s)
}

func Singular(s string) string {
	return plurClient.Singular(s)
}
func init() {
	strcase.ConfigureAcronym("API", "api")
	strcase.ConfigureAcronym("ID", "id")
	plurClient = pluralize.NewClient()
}
