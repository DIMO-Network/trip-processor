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

var topic string = "new-topic"
var partitions int32 = 9

type consumerGroupHandler struct {
	name string
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	uTrips := ongoingTrips{data: make(map[string]*userTrip)}
	for msg := range claim.Messages() {
		processTripsWithoutTimer(uTrips, msg)
		sess.MarkMessage(msg, "")
	}

	return nil
}

type userTrip struct {
	UserID         string
	Start          time.Time
	LatestTime     time.Time
	LatestSpeed    float64
	AutoExpireTrip *time.Timer
	Route          []coordinates
	mu             *sync.Mutex
}

type coordinates struct {
	Latitude  float64
	Longitude float64
}

type ongoingTrips struct {
	data map[string]*userTrip
	mu   *sync.Mutex
}

func (t ongoingTrips) Get(s string) (*userTrip, bool) {

	// t.mu.Lock()
	// defer t.mu.Unlock()

	val, ok := t.data[s]
	return val, ok
}

func (t ongoingTrips) Put(u *userTrip) {

	// t.mu.Lock()
	// defer t.mu.Unlock()
	t.data[u.UserID] = u
}

func (t ongoingTrips) Delete(s string) {

	// t.mu.Lock()
	// defer t.mu.Unlock()
	delete(t.data, s)

}

func (t ongoingTrips) AutoExpire(d time.Duration, s string) error {
	user, b := t.Get(s)
	if b != true {
		return fmt.Errorf("unable to get user trip for auto expire")
	}
	user.AutoExpireTrip = time.AfterFunc(d, func() {
		fmt.Printf("A completed trip has been logged.\n")
		t.Delete(s)
	})
	return nil
}

func (t ongoingTrips) RefreshAutoExpire(d time.Duration, s string) error {
	// t.mu.Lock()
	// defer t.mu.Unlock()
	user, b := t.Get(s)
	if b != true {
		return fmt.Errorf("unable to get user trip for refresh auto expire")
	}
	user.AutoExpireTrip.Reset(d)
	return nil
}

func (t ongoingTrips) StopAutoExpire(s string) error {
	// t.mu.Lock()
	// defer t.mu.Unlock()
	user, b := t.Get(s)
	if b != true {
		return fmt.Errorf("unable to get user trip for stop auto expire")
	}
	user.AutoExpireTrip.Stop()
	return nil
}

func (t ongoingTrips) RefreshLatestTime(tm time.Time, s string) error {
	// t.mu.Lock()
	// defer t.mu.Unlock()
	user, b := t.Get(s)
	if b != true {
		return fmt.Errorf("unable to get user trip for refresh latest time")
	}
	user.LatestTime = tm
	return nil
}

func (t ongoingTrips) AddCoordToRoute(coords coordinates, s string) error {
	// t.mu.Lock()
	// defer t.mu.Unlock()
	user, b := t.Get(s)
	if b != true {
		return fmt.Errorf("unable to get user trip for add coords to route")
	}
	user.Route = append(user.Route, coords)
	return nil
}

// k tables!!!!

func processTripsWithoutTimer(trps ongoingTrips, msg *sarama.ConsumerMessage) {

	timeVal := gjson.Get(string(msg.Value), "Timestamp").Raw
	currentSpeed := gjson.Get(string(msg.Value), "Speed").Float()
	lat := gjson.Get(string(msg.Value), "Latitude").Float()
	lon := gjson.Get(string(msg.Value), "Longitude").Float()

	currentTime, err := time.Parse("2006-01-02T15:04:05", timeVal)
	if err != nil {
		log.Fatal(err)
		return
	}

	coords := coordinates{Latitude: lat, Longitude: lon}

	if currentSpeed > 0 {
		if val, ok := trps.data[string(msg.Key)]; ok {
			if currentTime.Sub(val.LatestTime).Minutes() < 15 {
				if currentSpeed > 0.0 {
					trps.RefreshLatestTime(currentTime, val.UserID)
					trps.AddCoordToRoute(coords, val.UserID)
				}

			} else {
				fmt.Println("\n\nUser: ", string(msg.Key), "\nTrip Completed: ", val.Route)
				trps.Delete(val.UserID)

				newTrip := userTrip{Start: currentTime, LatestTime: currentTime, UserID: string(msg.Key), LatestSpeed: currentSpeed, Route: []coordinates{coords}}
				trps.Put(&newTrip)
				trps.AutoExpire(time.Minute*10, newTrip.UserID)
			}

		} else {
			newTrip := userTrip{Start: currentTime, LatestTime: currentTime, UserID: string(msg.Key), LatestSpeed: currentSpeed, Route: []coordinates{coords}}
			trps.Put(&newTrip)
			trps.AutoExpire(time.Minute*10, newTrip.UserID)
		}

	}
}

func handleErrors(group *sarama.ConsumerGroup, wg *sync.WaitGroup) {
	defer wg.Done()
	for err := range (*group).Errors() {
		fmt.Println("ERROR", err)
	}
}

func consume(group *sarama.ConsumerGroup, wg *sync.WaitGroup, name string) {
	fmt.Println(name + "start")
	defer wg.Done()
	ctx := context.Background()
	for {
		topics := []string{topic}
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
	config.Version = sarama.V2_8_1_0
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
