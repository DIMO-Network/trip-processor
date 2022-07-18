package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/tidwall/gjson"
)

type consumerGroupHandler struct {
	name string
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

type userTrip struct {
	Start          time.Time
	Latest         time.Time
	AutoExpireTrip *time.Timer
}

func (u *userTrip) AutoExpire(d time.Duration) {
	u.AutoExpireTrip = time.AfterFunc(d, func() {
		fmt.Printf("A completed trip has been logged.\n")
		// TO DO
		// remove key from map
	})
}

func (u *userTrip) RefreshAutoExpire(d time.Duration) {
	u.AutoExpireTrip.Reset(d)
}

func (u *userTrip) StopAutoExpire() bool {
	return u.AutoExpireTrip.Stop()
}

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	uTrips := make(map[string]userTrip)
	for msg := range claim.Messages() {
		// fmt.Printf("%s Message topic:%q partition:%d offset:%d  value:%s key:%q\n", h.name, msg.Topic, msg.Partition, msg.Offset, string(msg.Value), msg.Key)
		processTrips(uTrips, msg)
		sess.MarkMessage(msg, "")
	}

	return nil
}

func processTrips(ongoingTrips map[string]userTrip, msg *sarama.ConsumerMessage) {

	if val, ok := ongoingTrips[string(msg.Key)]; ok {

		if gjson.Get(string(msg.Value), "Speed").Int() > int64(0) {
			val.RefreshAutoExpire(time.Second * 10)
		} else {
			// just in here for testing
			time.Sleep(5 * time.Second)
		}
	} else {
		newTrip := userTrip{Start: time.Now(), Latest: time.Now()}
		newTrip.AutoExpire(time.Second * 10)
		ongoingTrips[string(msg.Key)] = newTrip
	}

}

func handleErrors(group *sarama.ConsumerGroup, wg *sync.WaitGroup) {
	wg.Done()
	for err := range (*group).Errors() {
		fmt.Println("ERROR", err)
	}
}

func consume(group *sarama.ConsumerGroup, wg *sync.WaitGroup, name string) {
	fmt.Println(name + "start")
	wg.Done()
	ctx := context.Background()
	for {
		topics := []string{"test-topic"}
		handler := consumerGroupHandler{name: name}
		err := (*group).Consume(ctx, topics, handler)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	var wg sync.WaitGroup
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = false
	config.Version = sarama.V0_10_2_0
	client, err := sarama.NewClient([]string{"localhost:9093"}, config)
	defer client.Close()
	if err != nil {
		log.Fatal(err)
	}
	group1, err := sarama.NewConsumerGroupFromClient("c1", client)
	if err != nil {
		log.Fatal(err)
	}
	group2, err := sarama.NewConsumerGroupFromClient("c2", client)
	if err != nil {
		log.Fatal(err)
	}
	group3, err := sarama.NewConsumerGroupFromClient("c3", client)
	if err != nil {
		log.Fatal(err)
	}
	group4, err := sarama.NewConsumerGroupFromClient("c4", client)
	if err != nil {
		log.Fatal(err)
	}
	group5, err := sarama.NewConsumerGroupFromClient("c5", client)
	if err != nil {
		log.Fatal(err)
	}
	group6, err := sarama.NewConsumerGroupFromClient("c6", client)
	if err != nil {
		log.Fatal(err)
	}
	group7, err := sarama.NewConsumerGroupFromClient("c7", client)
	if err != nil {
		log.Fatal(err)
	}
	group8, err := sarama.NewConsumerGroupFromClient("c8", client)
	if err != nil {
		log.Fatal(err)
	}
	group9, err := sarama.NewConsumerGroupFromClient("c9", client)
	if err != nil {
		log.Fatal(err)
	}
	defer group1.Close()
	defer group2.Close()
	defer group3.Close()
	defer group4.Close()
	defer group5.Close()
	defer group6.Close()
	defer group7.Close()
	defer group8.Close()
	defer group9.Close()
	wg.Add(9)
	go consume(&group1, &wg, "c1")
	go consume(&group2, &wg, "c2")
	go consume(&group3, &wg, "c3")
	go consume(&group4, &wg, "c4")
	go consume(&group5, &wg, "c5")
	go consume(&group6, &wg, "c6")
	go consume(&group7, &wg, "c7")
	go consume(&group8, &wg, "c8")
	go consume(&group9, &wg, "c9")
	wg.Wait()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	select {
	case <-signals:
	}
}
