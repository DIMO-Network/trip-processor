package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DIMO-Network/trips-api/internal/config"
	"github.com/DIMO-Network/trips-api/services/consumer"
	es "github.com/DIMO-Network/trips-api/services/es_client"
	"github.com/DIMO-Network/trips-api/services/segmenter"
	"github.com/DIMO-Network/trips-api/services/uploader"
	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/lovoo/goka"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/DIMO-Network/shared"
	"github.com/rs/zerolog"
)

const tripGracePeriod = 5 * time.Minute

var deviceStatusCodec = &shared.JSONCodec[shared.CloudEvent[segmenter.PartialStatusData]]{}
var segmentStateCodec = &shared.JSONCodec[segmenter.State]{}
var segmentEventCodec = &shared.JSONCodec[shared.CloudEvent[segmenter.SegmentEvent]]{}

func main() {
	ctx := context.Background()
	logger := zerolog.New(os.Stdout).With().Str("app", "segment-processor").Logger()

	settings, err := shared.LoadConfig[config.Settings]("settings.yaml")
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed loading settings.")
	}
	logger.Info().Interface("settings", settings).Msg("Settings loaded.")

	segmenter := segmenter.New(&logger, tripGracePeriod, &settings)

	serveMonitoring(settings.MonPort, &logger)

	brokers := strings.Split(settings.KafkaBrokers, ",")

	g := goka.DefineGroup(
		goka.Group(settings.ConsumerGroup),
		goka.Input(goka.Stream(settings.DeviceStatusTopic), deviceStatusCodec, segmenter.Process),
		goka.Persist(segmentStateCodec),
		goka.Output(goka.Stream(settings.TripEventTopic), segmentEventCodec),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create processor.")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			logger.Fatal().Err(err).Msg("Processor terminated with an error.")
		} else {
			logger.Info().Msg("Processor shut down cleanly.")
		}
	}()

	esClient, err := es.New(&settings)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to establish connection to elasticsearch.")
	}

	uploader, err := uploader.New(&settings)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to start Bunldr uploader")
	}

	consumer, err := consumer.New(esClient, uploader, &settings, &logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create consumer.")
	}

	consumer.Start(ctx)

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
