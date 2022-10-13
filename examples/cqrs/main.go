package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	// "github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/hnhuaxi/domain"
	"github.com/hnhuaxi/domain/driver/amqp"
	"github.com/hnhuaxi/domain/driver/nats"
	"github.com/hnhuaxi/domain/driver/nsq"
	"github.com/hnhuaxi/domain/messagebus"
	"github.com/nats-io/stan.go"
)

var (
	driver      string
	mode        int
	amqpAddress = "amqp://guest:guest@localhost:5672/"
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

func init() {
	flag.StringVar(&driver, "driver", "kafka", "选择使用消息队列的驱动(gochannel, kafka, nsq)")
	flag.IntVar(&mode, "mode", 0, "模式，启动的工作机制，(0: both, 1: publisher, 2: subscriber)")
}

func main() {
	flag.Parse()

	var (
		publisherMaker domain.PublisherMaker
		subscribeMaker domain.SubscriberMaker
	)

	switch driver {
	case "gochannel":
		publisherMaker, subscribeMaker = domain.GoPubsublisherMaker(domain.GochannelConfig{})
	case "kafka":

		publisherMaker = domain.KafkaPublisherMaker(kafka.PublisherConfig{
			Brokers:   []string{"localhost:9092"},
			Marshaler: kafka.DefaultMarshaler{},
		})
		subscribeMaker = domain.KafkaSubscriberMaker(kafka.SubscriberConfig{
			Brokers:       []string{"localhost:9092"},
			Unmarshaler:   kafka.DefaultMarshaler{},
			ConsumerGroup: fmt.Sprintf("test%d", 0 /* os.Getpid()*/),
		})
	case "nats":
		publisherMaker = nats.NatsPublisherMaker(nats.StreamingPublisherConfig{
			ClusterID: "cluster",
			ClientID:  "",
			StanOptions: []stan.Option{
				stan.NatsURL("nats://localhost:4222"),
			},
			Marshaler: nats.GobMarshaler{},
		})
		subscribeMaker = nats.NatsSubscriberMaker(nats.StreamingSubscriberConfig{
			ClusterID:        "test-cluster-test",
			ClientID:         "asdfasdfasdf", //"test-1",
			QueueGroup:       "test",
			DurableName:      "my-durable",
			SubscribersCount: 4,
			CloseTimeout:     time.Minute,
			AckWaitTimeout:   time.Second * 30,
			StanOptions: []stan.Option{
				stan.NatsURL("nats://localhost:4222"),
			},
			Unmarshaler: nats.GobMarshaler{},
		})
	case "nsq":
		publisherMaker = nsq.NsqPublisherMaker(nsq.NsqPublisherConfig{
			Addr:      "localhost:4150",
			Marshaler: nsq.GobMarshaler{},
		})
		subscribeMaker = nsq.NsqSubscriberMaker(nsq.NsqSubscribeConfig{
			Addr:        "localhost:4150",
			Channel:     "test",
			Unmarshaler: nsq.GobMarshaler{},
		})
	case "rabbitmq":
		publisherMaker = amqp.PublisherMaker(amqpAddress)
		subscribeMaker = amqp.SubscriberMaker(amqpAddress)
	default:
		panic("无效的消息队列驱动")
	}

	msgbus1 := messagebus.NewMessageBus(messagebus.BusConfig{
		SubscriberMaker: subscribeMaker,
		PublisherMaker:  publisherMaker,
		EventsName:      "example",
	})

	msgbus2 := messagebus.NewMessageBus(messagebus.BusConfig{
		SubscriberMaker: subscribeMaker,
		PublisherMaker:  publisherMaker,
		EventsName:      "example",
	})

	// msgbus1.AddCmdHandler(domain.NewCmdHandler(func(ctx context.Context, cmd *BookRoom) error {
	// 	log.Printf("BookRoom: %+v", cmd)
	// 	return msgbus1.EventBus().Publish(ctx, &OrderBeer{
	// 		RoomId: cmd.RoomId,
	// 		Count:  2,
	// 	})
	// }))

	msgbus1.AddCmdHandlerMaker(func(commandBus *domain.CommandBus, eventBus *domain.EventBus) domain.CommandHandler {
		return domain.NewCmdHandler(func(ctx context.Context, cmd *BookRoom) error {
			log.Printf("BookRoom: %+v", cmd)
			return msgbus1.EventBus().Publish(ctx, &OrderBeer{
				RoomId: cmd.RoomId,
				Count:  2,
			})
		})
	})

	msgbus1.AddEventHandlerMaker(func(commandBus *domain.CommandBus, eventBus *domain.EventBus) domain.EventHandler {
		return domain.NewEventHandler(func(ctx context.Context, evt *OrderBeer) error {
			log.Printf("OrderBeer: %+v", evt)
			return msgbus2.CommandBus().Send(ctx, &CleanRoom{
				RoomId: evt.RoomId,
			})
		})
	})
	// msgbus1.AddEventHandler(domain.NewEventHandler(func(ctx context.Context, evt *OrderBeer) error {
	// 	log.Printf("OrderBeer: %+v", evt)
	// 	return msgbus2.CommandBus().Send(ctx, &CleanRoom{
	// 		RoomId: evt.RoomId,
	// 	})
	// }))

	msgbus2.AddCmdHandler(domain.NewCmdHandler(func(ctx context.Context, cmd *CleanRoom) error {
		log.Printf("ClearRoom %+v", cmd)
		return nil
	}))

	msgbus2.AddEventHandler(domain.NoEventHandler)

	switch mode {
	case 0:
		go publishCommands(msgbus1.CommandBus())
		go msgbus2.Run(context.Background())

		err := msgbus1.Run(context.Background())
		if err != nil {
			log.Fatalf("exit for central run %s", err)
		}
	case 1:
		publishCommands(msgbus1.CommandBus())
	case 2:
		go msgbus2.Run(context.Background())

		err := msgbus1.Run(context.Background())
		if err != nil {
			log.Fatalf("exit for central run %s", err)
		}
	}

}

func publishCommands(commandBus *domain.CommandBus) func() {
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
