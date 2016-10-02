package main

import (
	"encoding/json"
	"github.com/parallelstream/redis-stream-processing-sample/domain"
	"gopkg.in/redis.v4"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	Info          *log.Logger
	MarginByStore = map[string]float32{
		"wallmart": 0.1,
		"netto":    0.2,
	}
)

type Enricher struct {
	Client        *redis.Client
	NotifyChannel string
	IncomingQueue string
	WorkingQueue  string
	OutQueue      string
	workPoolSet   string
	pubsub        *redis.PubSub
}

func (self *Enricher) start() {
	go func() {
		for {
			msg, err := self.pubsub.ReceiveMessage()
			if err != nil {
				panic(err)
			}
			self.processMsg(msg)
		}
	}()
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	ordersType := "retail"
	if len(os.Args) > 1 {
		ordersType = os.Args[1]
	}
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	channel := "orders.incoming." + ordersType
	pubsub, err := client.Subscribe(channel)
	defer pubsub.Close()
	if err != nil {
		panic(err)
	}
	enricher := Enricher{
		Client:        client,
		NotifyChannel: channel,
		IncomingQueue: channel,
		WorkingQueue:  "working." + channel,
		workPoolSet:   "workpool",
		pubsub:        pubsub,
	}
	Info = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)

	Info.Println("Start enricher for " + channel)
	enricher.start()
	wg.Wait()
}

func (self *Enricher) processMsg(msg *redis.Message) {
	orderJson, err := self.Client.BRPopLPush(self.IncomingQueue, self.WorkingQueue, time.Duration(0)*time.Second).Result()
	if err != nil {
		panic(err)
	}
	Info.Println("Fetched order " + orderJson)
	var order domain.Order
	err = json.Unmarshal([]byte(orderJson), &order)
	if err != nil {
		panic(err)
	}
	self.publishDeal(self.makeDeal(&order), order.Store, "deals."+order.Store)
	self.Client.LRem(self.WorkingQueue, 1, orderJson)
}

func (self *Enricher) publishDeal(deal *domain.ClientDeal, store string, outQueue string) {
	dealJson, err := json.Marshal(deal)
	_, err = self.Client.RPush(outQueue, dealJson).Result()
	if err != nil {
		panic(err)
	}
	self.Client.SAdd(self.workPoolSet, store)
	_, err = self.Client.Publish("deals", string(dealJson)).Result()
	if err != nil {
		panic(err)
	}
}

func (self *Enricher) makeDeal(order *domain.Order) *domain.ClientDeal {
	var deal domain.ClientDeal
	deal.OrderId = order.OrderId
	deal.CustomerId = order.CustomerId
	deal.CustomerName = strings.ToTitle(order.CustomerId[0:4])
	deal.CustomerType = order.CustomerType
	deal.Store = order.Store
	deal.TotalOrderSum = (1 + MarginByStore[order.Store]) * order.Price
	return &deal

}
