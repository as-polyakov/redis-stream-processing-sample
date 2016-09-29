package main

import (
    "fmt"
    "gopkg.in/redis.v4"
    "encoding/json"
    "github.com/satori/go.uuid"
    "time"
    "math/rand"
)

type order struct {
    OrderId string
    CustomerId string
    CustomerType string
    Store string
    Price float32
}

func main() {
    client := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
        Password: "",
        DB: 0,
    })

    _,err := client.Ping().Result()

    for {
        orderJson, _ := json.Marshal(produceOrder())
        fmt.Println(string(orderJson))
        _, err = client.RPush("orders.incoming", orderJson).Result()
        if(err != nil) {
            panic(err)
        }
        time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
    }

}
func produceOrder() (*order) {
    return &order{ OrderId: uuid.NewV1().String(),
    CustomerId: uuid.NewV1().String(),
    CustomerType: "retail",
    Store: "wallmart",
    Price: 0.1,
}
}


