package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	_ "github.com/lib/pq"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/tidwall/gjson"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "new-topic"
	group   goka.Group  = "mini-group"

	tmc *goka.TopicManagerConfig
)

func init() {
	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1

}

type trips struct {
	Trips       *ongoingTrips
	GokaContext goka.Context
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
	Timestamp time.Time
}

type ongoingTrips struct {
	data  map[string]*userTrip
	count int
	mu    sync.Mutex
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

// process messages until ctrl-c is pressed
func runProcessor(trps *ongoingTrips) {
	// process callback is invoked for each message delivered from
	cb := func(trps *ongoingTrips) goka.ProcessCallback {
		return func(ctx goka.Context, msg interface{}) {

			timeVal := gjson.Get(msg.(string), "Start").Str
			currentSpeed := gjson.Get(msg.(string), "LatestSpeed").Float()
			lat := gjson.Get(msg.(string), "Latitude").Float()
			lon := gjson.Get(msg.(string), "Longitude").Float()

			var currentTime time.Time
			var err error
			if timeVal != "" {
				currentTime, err = time.Parse("2006-01-02T15:04:05", timeVal)
				if err != nil {
					log.Fatal(err)
					return
				}
			} else {
				currentTime = time.Time{}
			}
			// determining if a coords reading is part of an earlier trip (timestamp > 15 min but coords are significantly diff than last time)
			coords := coordinates{Latitude: lat, Longitude: lon, Timestamp: currentTime}
			if currentSpeed > 0 {
				if val, ok := trps.data[ctx.Key()]; ok {
					if currentTime.Sub(val.LatestTime).Minutes() < 15 {
						if currentSpeed > 0.0 {
							trps.RefreshLatestTime(currentTime, val.UserID)
							trps.AddCoordToRoute(coords, val.UserID)
						}

					} else {
						fmt.Println("\n\nUser: ", ctx.Key(), "\nTrip Completed: ", val.Route)

						psqlInfo := fmt.Sprintf(
							"host=localhost port=5433 user=postgres password=postgres dbname=pg_db sslmode=disable",
						)
						db, err := sql.Open("postgres", psqlInfo)
						if err != nil {
							panic(err)
						}
						err = db.Ping()
						if err != nil {
							panic(err)
						}
						for n := 0; n < len(val.Route); n++ {

							geometry := fmt.Sprintf("POINT(%f %f)", val.Route[n].Longitude, val.Route[n].Latitude)
							if val.Route[n].Longitude != 0.0 && val.Route[n].Latitude != 0.0 {
								query := `INSERT INTO trips (devicekey, geom, pointnum, coord_timestamp, speed, tripid) VALUES ($1, $2, $3, $4, $5, $6)`
								_, err := db.Exec(query, ctx.Key(), geometry, n, val.Route[n].Timestamp, val.LatestSpeed, trps.count)
								if err != nil {
									fmt.Println(err)
								}
							} else {
								fmt.Println("coords were null? ", msg.(string))
							}
						}
						trps.count++
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
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // read all messages starting with oldest
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
	runProcessor(&uTrips)
}
