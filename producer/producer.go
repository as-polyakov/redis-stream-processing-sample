package main

import (
	"encoding/json"
	"fmt"
	"github.com/parallelstream/redis-stream-processing-sample/domain"
	"github.com/satori/go.uuid"
	"gopkg.in/redis.v4"
	"math/rand"
	"time"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err := client.Ping().Result()

	for {
		order := produceOrder()
		channel := "orders.incoming." + order.CustomerType
		orderJson, _ := json.Marshal(order)
		fmt.Println(string(orderJson))
		_, err = client.RPush(channel, orderJson).Result()
		if err != nil {
			panic(err)
		}
		err = client.Publish(channel, string(orderJson)).Err()
		time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	}

}
func produceOrder() *domain.Order {
	custType := "retail"
	if rand.Int31n(2) > 0 {
		custType = "corporate"
	}
	store := "wallmart"
	if custType == "retail" {
		store = "netto"
	}

	return &domain.Order{OrderId: uuid.NewV1().String(),
		CustomerId:   uuid.NewV1().String(),
		CustomerType: custType,
		Store:        store,
		Price:        rand.Float32() * 100,
	}
}
