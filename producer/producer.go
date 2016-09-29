package main

import (
	"encoding/json"
	"fmt"
	"github.com/satori/go.uuid"
	"gopkg.in/redis.v4"
	"math/rand"
	"time"
)

type order struct {
	OrderId      string
	CustomerId   string
	CustomerType string
	Store        string
	Price        float32
}

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
func produceOrder() *order {
	custType := "retail"
	if rand.Int31n(2) > 0 {
		custType = "corporate"
	}

	return &order{OrderId: uuid.NewV1().String(),
		CustomerId:   uuid.NewV1().String(),
		CustomerType: custType,
		Store:        "wallmart",
		Price:        rand.Float32() * 100,
	}
}
