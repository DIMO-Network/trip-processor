package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DIMO-Network/shared"
	_ "github.com/lib/pq"
	"github.com/lovoo/goka"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "topic.device.status"
	group   goka.Group  = "mini-group"
	event   goka.Stream = "topic.device.trip.event"

	tmc *goka.TopicManagerConfig

	pgController PostgresController
)

type Data struct {
	Data struct {
		ChargeLimit float64   `json:"chargeLimit,omitempty"`
		Odometer    float64   `json:"odometer,omitempty"`
		Year        int       `json:"year,omitempty"`
		Soc         float64   `json:"soc,omitempty"`
		Latitude    float64   `json:"latitude,omitempty"`
		Charging    bool      `json:"charging,omitempty"`
		Range       float64   `json:"range,omitempty"`
		Speed       float64   `json:"speed,omitempty"`
		Model       string    `json:"model,omitempty"`
		VehicleID   string    `json:"vehicleId,omitempty"`
		Make        string    `json:"make,omitempty"`
		Longitude   float64   `json:"longitude,omitempty"`
		Timestamp   time.Time `json:"timestamp,omitempty"`
		LatestTime  time.Time
		Start       time.Time
	} `json:"data,omitempty"`
	Route []tripPointinTime
}

type tripPointinTime struct {
	Latitude  float64 `json:"latitude,omitempty"`
	Longitude float64 `json:"longitude,omitempty"`
	Timestamp time.Time
	Speed     float64
}

type DeviceTrip struct {
	Id         string
	Start      time.Time
	End        time.Time
	LastActive time.Time
	LastIdle   time.Time
}

type PostgresController struct {
	db *sql.DB
}

type TripProcessor struct {
	Emitter *goka.Emitter
}

func init() {
	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1

	// psqlInfo := fmt.Sprintf(
	// 	"host=localhost port=5433 user=postgres password=postgres dbname=pg_db sslmode=disable",
	// )
	// var err error
	// pgController.db, err = sql.Open("postgres", psqlInfo)
	// if err != nil {
	// 	panic(err)
	// }
	// err = pgController.db.Ping()
	// if err != nil {
	// 	panic(err)
	// }
}

func (pgc *PostgresController) StoreTrip(trp DeviceTrip) error {

	query := `INSERT INTO fulltrips (id, tripstart, tripend) VALUES ($1, $2, $3) `
	_, err := pgc.db.Exec(query, trp.Id, trp.Start, trp.End)
	if err != nil {
		return err
	}
	return nil
}

var deviceStatusCodec = &shared.JSONCodec[Data]{}
var tripStateCodec = &shared.JSONCodec[DeviceTrip]{}

const TripGracePeriod = 15 * time.Minute

func (processor *TripProcessor) processDeviceStatus(ctx goka.Context, msg any) {
	var existingTrip *DeviceTrip
	newDeviceStatus := msg.(*Data)

	if val := ctx.Value(); val != nil {
		existingTrip = val.(*DeviceTrip)

		if newDeviceStatus.Data.Timestamp.Sub(existingTrip.LastActive) <= TripGracePeriod {
			if newDeviceStatus.Data.Speed > 0 {
				existingTrip.LastActive = newDeviceStatus.Data.Timestamp
				ctx.SetValue(existingTrip)
			}
		} else {
			// Grace period for the existing trip has passed, so need to end it before
			// doing anything else.
			// Trying to squash trips consisting of one data point.
			if existingTrip.Start != existingTrip.LastActive {
				existingTrip.End = existingTrip.LastActive
				processor.Emitter.Emit(ctx.Key(), existingTrip)
				ctx.Delete()
			}
			if newDeviceStatus.Data.Speed > 0 {
				ts := newDeviceStatus.Data.Timestamp.UTC()
				beginTrip := &DeviceTrip{
					Id:         ctx.Key(),
					Start:      ts,
					LastActive: ts,
				}
				ctx.SetValue(beginTrip)
				// TODO: Emit trip start event.
			}
		}

	} else {
		ts := newDeviceStatus.Data.Timestamp.UTC()
		beginTrip := &DeviceTrip{
			Id:         ctx.Key(),
			Start:      ts,
			LastActive: ts,
		}
		ctx.SetValue(beginTrip)
	}
}

// process messages until ctrl-c is pressed
func (processor *TripProcessor) runProcessor() {
	// Define a new processor group. The group defines all inputs, outputs, and
	// serialization formats. The group-table topic is "example-group-table".
	g := goka.DefineGroup(group,
		goka.Input(topic, deviceStatusCodec, processor.processDeviceStatus),
		goka.Persist(tripStateCodec),
	)

	p, err := goka.NewProcessor(brokers, g,
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder))
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			log.Fatalf("error running processor: %v", err)
		} else {
			log.Printf("Processor shutdown cleanly")
		}
	}()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait   // wait for SIGINT/SIGTERM
	cancel() // gracefully stop processor
	<-done
}

func main() {
	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	defer tm.Close()
	err = tm.EnsureStreamExists(string(topic), 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", topic, err)
	}

	emitter, err := goka.NewEmitter(brokers, event, tripStateCodec)
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish()

	tp := TripProcessor{Emitter: emitter}
	tp.runProcessor() // press ctrl-c to stop
}
