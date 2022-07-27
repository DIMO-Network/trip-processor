package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/tidwall/gjson"
)

var (
	brokers             = []string{"localhost:9093"}
	topic   goka.Stream = "new-topic"
	group   goka.Group  = "example-group"

	tmc *goka.TopicManagerConfig
)

func init() {
	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1

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
	mu   sync.Mutex
}

func (t *ongoingTrips) Get(s string) (*userTrip, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	val, ok := t.data[s]
	return val, ok
}

func (t *ongoingTrips) Put(u *userTrip) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data[u.UserID] = u
}

func (t *ongoingTrips) Delete(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.data, s)

}

func (t *ongoingTrips) AutoExpire(d time.Duration, s string) error {
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

func (t *ongoingTrips) RefreshAutoExpire(d time.Duration, s string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	user, b := t.Get(s)
	if b != true {
		return fmt.Errorf("unable to get user trip for refresh auto expire")
	}
	user.AutoExpireTrip.Reset(d)
	return nil
}

func (t *ongoingTrips) StopAutoExpire(s string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	user, b := t.Get(s)
	if b != true {
		return fmt.Errorf("unable to get user trip for stop auto expire")
	}
	user.AutoExpireTrip.Stop()
	return nil
}

func (t *ongoingTrips) RefreshLatestTime(tm time.Time, s string) error {
	user, b := t.Get(s)
	if b != true {
		return fmt.Errorf("unable to get user trip for refresh latest time")
	}
	user.LatestTime = tm
	return nil
}

func (t *ongoingTrips) AddCoordToRoute(coords coordinates, s string) error {
	user, b := t.Get(s)
	if b != true {
		return fmt.Errorf("unable to get user trip for add coords to route")
	}
	user.Route = append(user.Route, coords)
	return nil
}

// k tables!!!!

func processTripsWithoutTimer(trps *ongoingTrips, msg *sarama.ConsumerMessage) {

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

// process messages until ctrl-c is pressed
func runProcessor(trps *ongoingTrips) {
	// process callback is invoked for each message delivered from
	// "example-stream" topic.
	// cb := func(ctx goka.Context, msg interface{}) {
	// 	var counter int64
	// 	// ctx.Value() gets from the group table the value that is stored for
	// 	// the message's key.
	// 	if val := ctx.Value(); val != nil {
	// 		counter = val.(int64)
	// 	}
	// 	counter++
	// 	// SetValue stores the incremented counter in the group table for in
	// 	// the message's key.
	// 	ctx.SetValue(counter)
	// 	log.Printf("key = %s, counter = %v,partition = %v, msg = %v", ctx.Key(), counter, ctx.Partition(), msg)
	// }

	cb := func(trps *ongoingTrips) goka.ProcessCallback {
		return func(ctx goka.Context, msg interface{}) {

			timeVal := gjson.Get(msg.(string), "Timestamp").Raw
			currentSpeed := gjson.Get(msg.(string), "Speed").Float()
			lat := gjson.Get(msg.(string), "Latitude").Float()
			lon := gjson.Get(msg.(string), "Longitude").Float()

			currentTime, err := time.Parse("2006-01-02T15:04:05", timeVal)
			if err != nil {
				log.Fatal(err)
				return
			}

			coords := coordinates{Latitude: lat, Longitude: lon}

			if currentSpeed > 0 {
				if val, ok := trps.data[ctx.Key()]; ok {
					if currentTime.Sub(val.LatestTime).Minutes() < 15 {
						if currentSpeed > 0.0 {
							trps.RefreshLatestTime(currentTime, val.UserID)
							trps.AddCoordToRoute(coords, val.UserID)
						}

					} else {
						fmt.Println("\n\nUser: ", ctx.Key(), "\nTrip Completed: ", val.Route)
						trps.Delete(val.UserID)

						newTrip := userTrip{Start: currentTime, LatestTime: currentTime, UserID: ctx.Key(), LatestSpeed: currentSpeed, Route: []coordinates{coords}}
						trps.Put(&newTrip)
						trps.AutoExpire(time.Minute*10, newTrip.UserID)
					}

				} else {
					newTrip := userTrip{Start: currentTime, LatestTime: currentTime, UserID: ctx.Key(), LatestSpeed: currentSpeed, Route: []coordinates{coords}}
					trps.Put(&newTrip)
					trps.AutoExpire(time.Minute*10, newTrip.UserID)
				}

			}
		}
	}

	// Define a new processor group. The group defines all inputs, outputs, and
	// serialization formats. The group-table topic is "example-group-table".
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), cb(trps)),
		goka.Persist(new(codec.Int64)),
	)

	p, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
	)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			log.Printf("error running processor: %v", err)
		}
	}()

	sigs := make(chan os.Signal)
	go func() {
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	}()

	select {
	case <-sigs:
	case <-done:
	}
	cancel()
	<-done
}

func main() {
	config := goka.DefaultConfig()
	// since the emitter only emits one message, we need to tell the processor
	// to read from the beginning
	// As the processor is slower to start than the emitter, it would not consume the first
	// message otherwise.
	// In production systems however, check whether you really want to read the whole topic on first start, which
	// can be a lot of messages.
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	goka.ReplaceGlobalConfig(config)

	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	defer tm.Close()
	err = tm.EnsureStreamExists(string(topic), 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", topic, err)
	}
	uTrips := ongoingTrips{data: make(map[string]*userTrip)}
	runProcessor(&uTrips) // press ctrl-c to stop
}
