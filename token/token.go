package token

import (
	"time"

	"github.com/hnhuaxi/domain/registry"
)

type Token interface {
	AccessToken() string
	AccessTokenExpires() time.Time
	RefreshToken() string
	RefreshTokenExpires() time.Time
}

type TokenFactory interface {
	Token(node any) Token
	Refresh(token Token) error
}

var tokenRegistry = &registry.Registry[TokenFactory]{}

func RegisterFactory(node any, ctor func() TokenFactory) {
	tokenRegistry.Register(node, ctor)
}

func LookupFactory(node any) (func() TokenFactory, bool) {
	factory, ok := tokenRegistry.Lookup(node)
	return factory, ok
}

func LookupRefresher(node any) (TokenFactory, bool) {
	factory, ok := tokenRegistry.Lookup(node)
	if !ok {
		return nil, false
	}
	return factory(), true
}

func Load(node any) (Token, bool) {
	ctor, ok := LookupFactory(node)
	if !ok {
		return nil, false
	}

	tok := ctor().Token(node)
	return tok, tok != nil
}
