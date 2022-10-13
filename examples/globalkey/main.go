package main

import (
	"flag"
	"log"

	"github.com/go-redis/redis/v8"
	"github.com/hnhuaxi/domain/globalkey"
)

type User struct {
	ID   uint
	Name string
}

var (
	addr string
	db   int
)

func init() {
	flag.StringVar(&addr, "addr", ":6379", "redis addr")
	flag.IntVar(&db, "db", 3, "redis db")
}

func main() {
	flag.Parse()
	var (
		rdb = redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   db,
		})
		key  = globalkey.NewGlobalKey[*User]("test$$struct", rdb)
		key1 = globalkey.NewGlobalKey[int]("test$$int", rdb)
		key2 = globalkey.NewGlobalKey[string]("test$$string", rdb)
	)

	// key.Store(&User{
	// 	ID:   10,
	// 	Name: "bob",
	// })

	u, ok := key.Load()
	if !ok {
		log.Fatalf("not found key")
	}
	log.Printf("user %v", u)

	key1.Store(1)

	i, ok := key1.Load()
	if !ok {
		log.Fatalf("not found key")
	}
	log.Printf("int %v", i)
	key2.Store("abc")

	s, ok := key2.Load()
	if !ok {
		log.Fatalf("not found key")
	}
	log.Printf("str %v", s)
}
