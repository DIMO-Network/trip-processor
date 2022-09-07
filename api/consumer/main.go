package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"trips-api/api/internal/config"

	"github.com/DIMO-Network/shared"
	"github.com/lovoo/goka"
	"github.com/rs/zerolog"
)

var (
	topic      goka.Stream = "topic.device.status"
	group      goka.Group  = "trip-processor"
	tripstatus goka.Stream = "topic.device.trip.event"
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

type DeviceTrip struct {
	Id         string
	Start      time.Time
	End        time.Time
	LastActive time.Time
	LastIdle   time.Time
}

type TripProcessor struct {
	logger *zerolog.Logger
}

type TripStatus struct {
	DeviceID string
	Start    time.Time
	End      time.Time
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
				ctx.Emit(tripstatus, ctx.Key(), TripStatus{DeviceID: ctx.Key(), Start: ts})
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
		ctx.Emit(tripstatus, ctx.Key(), TripStatus{DeviceID: ctx.Key(), Start: ts})
	}
}

// process messages until ctrl-c is pressed
func (tp *TripProcessor) runProcessor(brokers []string) {
	// Define a new processor group. The group defines all inputs, outputs, and
	// serialization formats. The group-table topic is "example-group-table".
	g := goka.DefineGroup(group,
		goka.Input(topic, deviceStatusCodec, tp.processDeviceStatus),
		goka.Persist(tripStateCodec),
		goka.Output(tripstatus, tripStatusCodec),
	)

	p, err := goka.NewProcessor(brokers, g)
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
		logger.Fatal().Err(err).Msg("Failed loading settings.")
	}
	logger.Info().Interface("settings", settings).Msg("Settings loaded.")

	tp := &TripProcessor{logger: &logger}
	brokers := strings.Split(settings.KafkaBrokers, ",")
	tp.runProcessor(brokers) // press ctrl-c to stop
}
