package messagebus

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/hnhuaxi/domain"
	"github.com/stretchr/testify/assert"
)

type BookRoom struct {
	RoomId    string
	GuestName string
	StartAt   time.Time
	EndAt     time.Time
}

type OrderBeer struct {
	RoomId string
	Count  int
}

type CleanRoom struct {
	RoomId string
}

func TestMain(m *testing.M) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	m.Run()
}

func TestBus(t *testing.T) {
	var (
		publisherMaker, subscribeMaker = domain.GoPubsublisherMaker(gochannel.Config{})
	)

	central1 := NewMessageBus(BusConfig{
		SubscriberMaker: subscribeMaker,
		PublisherMaker:  publisherMaker,
	})

	central1.AddCmdHandler(domain.NewCmdHandler(func(ctx context.Context, cmd *BookRoom) error {
		log.Printf("BookRoom: %+v", cmd)
		central1.EventBus().Publish(ctx, &OrderBeer{
			RoomId: cmd.RoomId,
			Count:  2,
		})
		return nil
	}))

	central1.AddEventHandler(domain.NewEventHandler(func(ctx context.Context, evt *OrderBeer) error {
		log.Printf("OrderBeer: %+v", evt)
		central1.CommandBus().Send(ctx, &CleanRoom{
			RoomId: evt.RoomId,
		})
		return nil
	}))

	err := central1.Run(context.Background())
	assert.NoError(t, err)
}

func publishCommands(commandBus *cqrs.CommandBus) func() {
	i := 0
	for {
		i++

		startDate := time.Now()

		endDate := time.Now().Add(time.Hour * 24 * 3)

		bookRoomCmd := &BookRoom{
			RoomId:    fmt.Sprintf("%d", i),
			GuestName: "John",
			StartAt:   startDate,
			EndAt:     endDate,
		}
		if err := commandBus.Send(context.Background(), bookRoomCmd); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}
