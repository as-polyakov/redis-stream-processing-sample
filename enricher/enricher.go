package main

import (
	"fmt"
	"gopkg.in/redis.v4"
	"os"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ordersType := "retail"
	if len(os.Args) > 1 {
		ordersType = os.Args[1]
	}
	channel := "orders.incoming." + ordersType
	pubsub, err := client.Subscribe(channel)
	defer pubsub.Close()

	if err != nil {
		panic(err)
	}
	for {
		msg, _ := pubsub.ReceiveMessage()
		processMsg(client, msg)
	}
}

func processMsg(client *redis.Client, msg *redis.Message) {
	fmt.Println(msg.Channel, msg.Payload)
}
