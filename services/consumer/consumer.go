package consumer

import (
	"context"
	"strings"
	"time"

	"github.com/DIMO-Network/shared"
	"github.com/DIMO-Network/shared/kafka"
	"github.com/DIMO-Network/trips-api/internal/config"
	es "github.com/DIMO-Network/trips-api/services/es_client"
	"github.com/DIMO-Network/trips-api/services/segmenter"
	"github.com/DIMO-Network/trips-api/services/uploader"
	"github.com/rs/zerolog"
)

type CompletedSegmentConsumer struct {
	config kafka.Config
	logger *zerolog.Logger
	es     *es.Client
}

func New(es *es.Client, settings *config.Settings, logger *zerolog.Logger) (*CompletedSegmentConsumer, error) {
	kc := kafka.Config{
		Brokers: strings.Split(settings.KafkaBrokers, ","),
		Topic:   settings.TripEventTopic,
		Group:   "segmenter",
	}

	return &CompletedSegmentConsumer{
		config: kc,
		logger: logger,
		es:     es,
	}, nil
}

func (c *CompletedSegmentConsumer) Start(ctx context.Context) {
	if err := kafka.Consume(ctx, c.config, c.ingest, c.logger); err != nil {
		c.logger.Fatal().Err(err).Msg("Couldn't start segment consumer.")
	}

	c.logger.Info().Msg("segment consumer started.")
}

func (c *CompletedSegmentConsumer) ingest(ctx context.Context, event *shared.CloudEvent[segmenter.SegmentEvent]) error {
	response, err := c.es.FetchData(event.Data.DeviceID, event.Data.Start.Format(time.RFC3339), event.Data.End.Format(time.RFC3339))
	if err != nil {
		return err
	}

	encryptedData, err := uploader.PrepareData(response, event.Data.DeviceID, event.Data.Start.Format(time.RFC3339), event.Data.End.Format(time.RFC3339))
	if err != nil {
		return err
	}

	return uploader.Upload(encryptedData)
}
