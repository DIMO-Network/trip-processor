package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
	"trips-api/api/internal/config"

	"github.com/DIMO-Network/shared"
	_ "github.com/lib/pq"
	"github.com/lovoo/goka"
	"github.com/rs/zerolog"
)

var (
	brokers                = []string{"localhost:9092"}
	topic      goka.Stream = "topic.device.status"
	group      goka.Group  = "mini-group"
	tripstatus goka.Stream = "topic.device.trip.event"

	tmc *goka.TopicManagerConfig
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

type TripProcessor struct {
	logger    *zerolog.Logger
	db        *sql.DB
	TripCount int
}

type TripStartEvent struct {
	Id    string
	Start time.Time
}

type TripStatus struct {
	DeviceID string
	Start    time.Time
	End      time.Time
}

func init() {
	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}

func (processor *TripProcessor) StoreTrip(trp TripStatus) error {

	if !trp.End.IsZero() {
		query := `INSERT INTO fulltrips (deviceid, tripstart, tripend, tripid) VALUES ($1, $2, $3, $4) `
		_, err := processor.db.Exec(query, trp.DeviceID, trp.Start, trp.End, processor.TripCount)
		if err != nil {
			return err
		}
		processor.TripCount++
	}
	return nil
}

var deviceStatusCodec = &shared.JSONCodec[Data]{}
var tripStateCodec = &shared.JSONCodec[DeviceTrip]{}
var tripStatusCodec = &shared.JSONCodec[TripStatus]{}

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
				ctx.Emit(tripstatus, ctx.Key(), TripStatus{DeviceID: ctx.Key(), Start: existingTrip.Start, End: existingTrip.End})
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
				ctx.Emit(tripstatus, ctx.Key(), TripStartEvent{Id: ctx.Key(), Start: ts})
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
		ctx.Emit(tripstatus, ctx.Key(), TripStartEvent{Id: ctx.Key(), Start: ts})
	}
}

func (processor *TripProcessor) listenForCompletedTrips(ctx goka.Context, msg any) {

	completedTrip := msg.(*TripStatus)
	err := processor.StoreTrip(*completedTrip)
	if err != nil {
		processor.logger.Err(err)
	}

}

// process messages until ctrl-c is pressed
func (tp *TripProcessor) runProcessor() {
	// Define a new processor group. The group defines all inputs, outputs, and
	// serialization formats. The group-table topic is "example-group-table".
	g := goka.DefineGroup(group,
		goka.Input(topic, deviceStatusCodec, tp.processDeviceStatus),
		goka.Input(tripstatus, tripStatusCodec, tp.listenForCompletedTrips),
		goka.Persist(tripStateCodec),
		goka.Output(tripstatus, tripStatusCodec),
	)

	p, err := goka.NewProcessor(brokers, g,
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder))
	if err != nil {
		tp.logger.Fatal().Err(err).Msg("Failed to create processor.")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			tp.logger.Fatal().Err(err).Msg("Processor terminated with an error.")
		} else {
			tp.logger.Info().Msg("Processor shut down cleanly.")
		}
	}()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait   // wait for SIGINT/SIGTERM
	cancel() // gracefully stop processor
	<-done
}

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Str("app", "trip-processor").Logger()

	settings, err := shared.LoadConfig[config.Settings]("settings.yaml")
	if err != nil {
		logger.Fatal().Err(err).Msg("could not load settings")
	}
	psqlInfo := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		settings.DBHost,
		settings.DBPort,
		settings.DBUser,
		settings.DBPassword,
		settings.DBName,
	)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	tp := &TripProcessor{logger: &logger, db: db}
	tp.runProcessor() // press ctrl-c to stop
}
