package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
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
	UserID         string
	Start          time.Time
	LatestTime     time.Time
	LastestSpeed   int64
	AutoExpireTrip *time.Timer
	TripComplete   bool
	VehicleStopped time.Time
}

func (u *userTrip) AutoExpire(d time.Duration, tripMap map[string]userTrip) {
	u.AutoExpireTrip = time.AfterFunc(d, func() {
		fmt.Printf("A completed trip has been logged.\n")
		delete(tripMap, u.UserID)
	})
}

func (u *userTrip) RefreshAutoExpire(d time.Duration) {
	u.AutoExpireTrip.Reset(d)
}

func (u *userTrip) StopAutoExpire() bool {
	return u.AutoExpireTrip.Stop()
}

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// how/ when will this be read/ written to? mutex lock?
	uTrips := make(map[string]userTrip)
	for msg := range claim.Messages() {
		// fmt.Printf("%s Message topic:%q partition:%d offset:%d  value:%s key:%q\n", h.name, msg.Topic, msg.Partition, msg.Offset, string(msg.Value), msg.Key)
		processTripsWithoutTimer(uTrips, msg)
		sess.MarkMessage(msg, "")
	}

	return nil
}

func processTripsWithTimer(ongoingTrips map[string]userTrip, msg *sarama.ConsumerMessage) {

	currentSpeed := gjson.Get(string(msg.Value), "Speed").Int()

	if currentSpeed > int64(0) {
		if val, ok := ongoingTrips[string(msg.Key)]; ok {

			if currentSpeed > int64(0) {
				val.RefreshAutoExpire(time.Second * 10)
				return
			} else {
				// sleep is just in here for testing
				time.Sleep(5 * time.Second)
			}
		} else {
			newTrip := userTrip{Start: time.Now(), LatestTime: time.Now(), UserID: string(msg.Key)}
			newTrip.AutoExpire(time.Second*10, ongoingTrips)
			ongoingTrips[string(msg.Key)] = newTrip
		}
	}
}

func processTripsWithoutTimer(ongoingTrips map[string]userTrip, msg *sarama.ConsumerMessage) {

	speedVal := gjson.Get(string(msg.Value), "Speed").Raw
	timeVal := gjson.Get(string(msg.Value), "Timestamp").Raw
	currentTime, err := time.Parse("2006-01-02T15:04:05", timeVal)
	if err != nil {
		log.Fatal(err)
		return
	}
	currentSpeed, err := strconv.Atoi(speedVal)
	if err != nil {
		log.Fatal(err)
		return
	}

	if val, ok := ongoingTrips[string(msg.Key)]; ok {
		if currentTime.Sub(val.LatestTime).Minutes() <= 15 && currentTime.Sub(val.LatestTime).Minutes() > 0 {

			if currentSpeed == 0 {
				if val.VehicleStopped.IsZero() {
					val.VehicleStopped = currentTime
					val.LatestTime = currentTime
					fmt.Println("\t\tvehicle stopped", currentSpeed, currentTime)
					return
				} else {
					if currentTime.Sub(val.VehicleStopped).Minutes() <= 15 && currentTime.Sub(val.VehicleStopped).Minutes() > 0 {
						val.TripComplete = true
						delete(ongoingTrips, val.UserID)
						fmt.Println("Trip Completed, vehicle stopped for more than 15 minutes\t", currentSpeed, currentTime)
						return
					}
					val.LatestTime = currentTime
					fmt.Println("\t", currentSpeed, currentTime)
					return
				}
			} else {
				if !val.VehicleStopped.IsZero() {
					val.VehicleStopped = time.Time{}
				}
				val.LatestTime = currentTime
				fmt.Println("\t", currentSpeed, currentTime)
				return
			}
		} else {

			if currentTime.Sub(val.LatestTime).Minutes() < 0 {
				fmt.Println("\t\tTime delta < 0", currentSpeed, currentTime)
				val.TripComplete = true
				delete(ongoingTrips, val.UserID)
				fmt.Println("Trip Completed, most recent timestamp is earlier than latest timestamp recorded\t", currentSpeed, currentTime)
				return
			}

			val.TripComplete = true
			delete(ongoingTrips, val.UserID)
			fmt.Println("Trip Completed, timestamp greater than 15 minutes after latest\t", currentSpeed, currentTime)
		}

	} else {
		if currentSpeed != 0 {
			fmt.Println("\nStarting new trip\t", currentSpeed, currentTime)
			newTrip := userTrip{Start: currentTime, LatestTime: currentTime, UserID: string(msg.Key)}
			ongoingTrips[string(msg.Key)] = newTrip
			return
		}
	}

	fmt.Println("\nMovement not detected\t", currentSpeed, currentTime)
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
	defer group1.Close()
	wg.Add(9)
	go consume(&group1, &wg, "c1")
	wg.Wait()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	select {
	case <-signals:
	}
}
