package main

import (
	"encoding/json"
	"github.com/parallelstream/redis-stream-processing-sample/domain"
	"gopkg.in/redis.v4"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	Info               *log.Logger
	WorkKeepTimeoutSec int = 3
)

type Aggregator struct {
	Client       *redis.Client
	pubsub       *redis.PubSub
	Id           string
	workPoolSet  string
	lockedWork   map[string]string
	lastByStore  map[string]int64
	capacity     int
	StoreResults map[string]*StoreResult
}

type StoreResult struct {
	Store            string
	TotalSalesAmount float32
	AvgAmount        float32
}
type PartialAggregation struct {
	TotalAmount float32
	DealsNum    int64
}

func (self *Aggregator) start() {
	self.tryStealWork()
	go func() {
		for {
			msg, err := self.pubsub.ReceiveMessage()
			if err != nil {
				panic(err)
			}
			store := self.getStore(msg)
			if _, ok := self.lockedWork[store]; ok {
				self.processTillLast(store, self.lastByStore[store])
				self.PrintSummary(store)
			}
		}
	}()
}

func (self *Aggregator) getStore(msg *redis.Message) string {
	var deal domain.ClientDeal
	err := json.Unmarshal([]byte(msg.Payload), &deal)
	if err != nil {
		panic(err)
	}
	return deal.Store
}

func (self *Aggregator) tryStealWork() {
	ticker := time.NewTicker(2 * time.Second)
	go func() {
		for {
			<-ticker.C
			if self.capacity > 0 {
				Info.Println("Fishing for new work with capacity " + strconv.Itoa(self.capacity))
				workItems, err := self.Client.SMembers(self.workPoolSet).Result()
				if err != nil {
					panic(err)
				}
				for _, workItem := range workItems {
					if self.capacity > 0 {
						res, _ := self.Client.SetNX(workItem, self.Id, time.Duration(WorkKeepTimeoutSec+1)*time.Second).Result()
						if res == true {
							self.launchWorkKeeper(workItem)
							self.initialLoad(workItem)
							Info.Println("Successfuly grabbed new work to do - " + workItem)
							self.lockedWork[workItem] = workItem
							self.capacity--
						}
					}
				}
			}

		}
	}()
}

func (self *Aggregator) launchWorkKeeper(workItem string) {
	ticker := time.NewTicker(time.Duration(WorkKeepTimeoutSec) * time.Second)
	go func() {
		for {
			<-ticker.C
			res, _ := self.Client.Expire(workItem, time.Duration(WorkKeepTimeoutSec+1)*time.Second).Result()
			if res != true {
				Info.Println("Lost lock for " + workItem)
			}
		}
	}()
}
func (self *Aggregator) processTillLast(workItem string, from int64) int64 {
	last, err := self.Client.LLen("deals." + workItem).Result()
	if err != nil {
		panic(err)
	}
	partialAgg := self.processDeals("deals."+workItem, 0, last)
	self.StoreResults[workItem] = &StoreResult{
		Store:            workItem,
		TotalSalesAmount: partialAgg.TotalAmount,
		AvgAmount:        partialAgg.TotalAmount / float32(partialAgg.DealsNum),
	}
	return last
}

func (self *Aggregator) PrintSummary(store string) {
	Info.Printf("%+v\n", self.StoreResults[store])
}

func (self *Aggregator) initialLoad(workItem string) {
	Info.Println("Initial loading " + workItem)
	last := self.processTillLast(workItem, 0)
	self.lastByStore[workItem] = last
	Info.Println("Done initial loading " + workItem)
	self.PrintSummary(workItem)
}

func (self *Aggregator) processDeals(queue string, from int64, to int64) *PartialAggregation {
	dealsJson, err := self.Client.LRange(queue, from, to).Result()
	if err != nil {
		panic(err)
	}
	deals := make([]domain.ClientDeal, len(dealsJson), len(dealsJson))
	for i, dealJson := range dealsJson {
		err := json.Unmarshal([]byte(dealJson), &deals[i])
		if err != nil {
			panic(err)
		}
	}
	return self.aggregate(deals)
}

func (self *Aggregator) aggregate(deals []domain.ClientDeal) *PartialAggregation {
	var totalSales float32
	for _, deal := range deals {
		totalSales += deal.TotalOrderSum
	}
	return &PartialAggregation{
		TotalAmount: totalSales,
		DealsNum:    int64(len(deals)),
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	var capacity int = 1
	var err error
	if len(os.Args) > 1 {
		capacity, err = strconv.Atoi(os.Args[1])
		if err != nil {
			panic(err)
		}
	}
	Info = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	pubsub, _ := client.Subscribe("deals")
	aggregator := Aggregator{
		Id:           strconv.Itoa(os.Getpid()),
		Client:       client,
		pubsub:       pubsub,
		capacity:     capacity,
		workPoolSet:  "workpool",
		lockedWork:   make(map[string]string),
		lastByStore:  make(map[string]int64),
		StoreResults: make(map[string]*StoreResult),
	}
	Info.Println("Staring aggregator " + aggregator.Id)
	aggregator.start()
	wg.Wait()
}
