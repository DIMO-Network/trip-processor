package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DIMO-Network/trips-api/internal/config"

	"github.com/DIMO-Network/shared"
	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/lovoo/goka"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/segmentio/ksuid"
)

type PartialStatusData struct {
	Speed     float64   `json:"speed"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Odometer  float64   `json:"odometer"`
	SOC       float64   `json:"soc"`
	Timestamp time.Time `json:"timestamp"`
}

type TripState struct {
	Start      time.Time `json:"start"`
	Idle       time.Time `json:"idle"`
	LastActive time.Time `json:"lastActive"`
}

type TripProcessor struct {
	logger         *zerolog.Logger
	tripEventTopic goka.Stream
}

type TripEvent struct {
	DeviceID string    `json:"deviceId"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
}

const tripGracePeriod = 15 * time.Minute

var deviceStatusCodec = &shared.JSONCodec[shared.CloudEvent[PartialStatusData]]{}
var tripStateCodec = &shared.JSONCodec[shared.CloudEvent[TripState]]{}
var tripEventCodec = &shared.JSONCodec[shared.CloudEvent[TripEvent]]{}

func (p *TripProcessor) processDeviceStatus(ctx goka.Context, msg any) {
	userDeviceID := ctx.Key()
	newDeviceStatus := msg.(*shared.CloudEvent[PartialStatusData])

	if val := ctx.Value(); val != nil {
		existingTrip := val.(*shared.CloudEvent[TripState])

		if newDeviceStatus.Data.Timestamp.Sub(existingTrip.Data.LastActive) <= tripGracePeriod {
			// new status came within the grace period of the last status

			if newDeviceStatus.Data.Speed == 0.0 && existingTrip.Data.Idle.IsZero() {
				// if trip is idle, note the start of the idle period
				existingTrip.Data.Idle = newDeviceStatus.Data.Timestamp
				return
			}

			// if trip is active, update last active timestamp and clear any idle state
			existingTrip.Data.LastActive = newDeviceStatus.Data.Timestamp
			existingTrip.Data.Idle = time.Time{}
			ctx.SetValue(existingTrip)
			return
		}

		// The grace period for the existing trip has passed. We must end it before potentially
		// starting a new one.
		existingTripEnd := &shared.CloudEvent[TripEvent]{
			ID:      ksuid.New().String(),
			Time:    time.Now(),
			Subject: userDeviceID,
			Type:    "zone.dimo.device.trip.event",
			Data: TripEvent{
				DeviceID: ctx.Key(),
				Start:    existingTrip.Data.Start,
			},
		}

		if !existingTrip.Data.Idle.IsZero() {
			// if idle status is observed, return start of idle as trip end
			existingTripEnd.Data.End = existingTrip.Data.Idle
			p.logger.Info().Str("userDeviceId", ctx.Key()).Time("start", existingTrip.Data.Start).Time("end", existingTrip.Data.Idle).Msg("Ending trip.")
		} else {
			// otherwise, set the trip end as the last active timestamp
			existingTripEnd.Data.End = existingTrip.Data.LastActive
			p.logger.Info().Str("userDeviceId", ctx.Key()).Time("start", existingTrip.Data.Start).Time("end", existingTrip.Data.LastActive).Msg("Ending trip.")
		}
		ctx.Emit(p.tripEventTopic, userDeviceID, existingTripEnd)
		ctx.Delete()
	}

	// Start a new trip if the device is moving.
	if newDeviceStatus.Data.Speed > 0 {
		p.logger.Info().Str("userDeviceId", ctx.Key()).Msg("Starting trip.")
		ts := newDeviceStatus.Data.Timestamp.UTC()

		newTripState := &shared.CloudEvent[TripState]{
			ID:      ksuid.New().String(),
			Time:    time.Now(),
			Subject: userDeviceID,
			Type:    "zone.dimo.device.trip.state",
			Data: TripState{
				Start:      ts,
				LastActive: ts,
			},
		}
		ctx.SetValue(newTripState)

		newTripEvent := &shared.CloudEvent[TripEvent]{
			ID:      ksuid.New().String(),
			Time:    time.Now(),
			Subject: userDeviceID,
			Type:    "zone.dimo.device.trip.event",
			Data: TripEvent{
				DeviceID: userDeviceID,
				Start:    ts,
			},
		}
		ctx.Emit(p.tripEventTopic, userDeviceID, newTripEvent)
	}
}

// process messages until ctrl-c is pressed
func (p *TripProcessor) runProcessor(settings *config.Settings) {
	brokers := strings.Split(settings.KafkaBrokers, ",")

	// Define a new processor group. The group defines all inputs, outputs, and
	// serialization formats. The group-table topic is "example-group-table".
	g := goka.DefineGroup(
		goka.Group(settings.ConsumerGroup),
		goka.Input(goka.Stream(settings.DeviceStatusTopic), deviceStatusCodec, p.processDeviceStatus),
		goka.Persist(tripStateCodec),
		goka.Output(goka.Stream(settings.TripEventTopic), tripEventCodec),
	)

	tp, err := goka.NewProcessor(brokers, g)
	if err != nil {
		p.logger.Fatal().Err(err).Msg("Failed to create processor.")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		defer close(done)
		if err = tp.Run(ctx); err != nil {
			p.logger.Fatal().Err(err).Msg("Processor terminated with an error.")
		} else {
			p.logger.Info().Msg("Processor shut down cleanly.")
		}
	}()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait   // wait for SIGINT/SIGTERM
	cancel() // gracefully stop processor
	<-done
}

func serveMonitoring(port string, logger *zerolog.Logger) *fiber.App {
	logger.Info().Str("port", port).Msg("Starting monitoring web server.")

	monApp := fiber.New(fiber.Config{DisableStartupMessage: true})

	monApp.Get("/", func(c *fiber.Ctx) error { return nil })
	monApp.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	go func() {
		if err := monApp.Listen(":" + port); err != nil {
			logger.Fatal().Err(err).Str("port", port).Msg("Failed to start monitoring web server.")
		}
	}()

	return monApp
}

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Str("app", "trip-processor").Logger()

	settings, err := shared.LoadConfig[config.Settings]("settings.yaml")
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed loading settings.")
	}
	logger.Info().Interface("settings", settings).Msg("Settings loaded.")

	serveMonitoring(settings.MonPort, &logger)

	tp := &TripProcessor{
		logger:         &logger,
		tripEventTopic: goka.Stream(settings.TripEventTopic),
	}
	tp.runProcessor(&settings) // press ctrl-c to stop
}
