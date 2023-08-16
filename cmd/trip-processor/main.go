package main

import (
	"context"
	"os"
	"strings"

	"github.com/DIMO-Network/shared/kafka"
	"github.com/DIMO-Network/trips-api/internal/config"
	"github.com/DIMO-Network/trips-api/services/segmenter"
	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/DIMO-Network/shared"
	"github.com/rs/zerolog"
)

func main() {
	ctx := context.Background()
	logger := zerolog.New(os.Stdout).With().Str("app", "segment-processor").Logger()

	settings, err := shared.LoadConfig[config.Settings]("settings.yaml")
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed loading settings.")
	}
	logger.Info().Interface("settings", settings).Msg("Settings loaded.")

	kc := kafka.Config{
		Brokers: strings.Split(settings.KafkaBrokers, ","),
		Topic:   settings.DeviceStatusTopic,
		Group:   "segment-consumer",
	}

	segmenter := segmenter.New(&logger, &settings)

	if err := kafka.Consume(ctx, kc, segmenter.Process, &logger); err != nil {
		logger.Fatal().Err(err).Msg("Couldn't start event consumer.")
	}

	logger.Info().Str("port", settings.MonPort).Msg("Starting monitoring web server.")

	monApp := fiber.New(fiber.Config{DisableStartupMessage: true})

	monApp.Get("/", func(c *fiber.Ctx) error { return nil })
	monApp.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	if err := monApp.Listen(":" + settings.MonPort); err != nil {
		logger.Fatal().Err(err).Str("port", settings.MonPort).Msg("Failed to start monitoring web server.")
	}

}
