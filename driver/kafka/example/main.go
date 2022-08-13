package main

import (
	"context"
	"log"

	"github.com/hnhuaxi/domain/driver/kafka"
)

func main() {
	maker := kafka.SubscriberMaker(kafka.SubscribeConfig{
		Brokers: []string{"localhost"},
		Group:   "test",
	})
	sub, err := maker()
	if err != nil {
		log.Fatalf("create subscriber error %s", err)
	}

	defer sub.Close()

	var ctx = context.Background()
	chmsg, err := sub.Subscribe(ctx, "stats-db.stats.tracks")
	if err != nil {
		log.Fatalf("subscribe topic '%s' error %s", "stats-db.stats.tracks", err)
	}

	log.Printf("waiting messages")
	for msg := range chmsg {
		log.Printf("msg %s", string(msg.Payload))
	}
}
