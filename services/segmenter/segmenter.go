package segmenter

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/DIMO-Network/shared"
	"github.com/DIMO-Network/trips-api/internal/config"
	"github.com/DIMO-Network/trips-api/services/haversine"
	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

const tripGracePeriod = 15 * time.Minute

type SegmentProcessor struct {
	logger      *zerolog.Logger
	segments    map[string]SegmentState
	producer    sarama.SyncProducer
	gracePeriod time.Duration
	topic       string
}

type PartialStatusData struct {
	Altitude  float64   `json:"altitude"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	RunTime   int64     `json:"runTime"`
	Speed     float64   `json:"speed"`
	Odometer  float64   `json:"odometer"`
	SOC       float64   `json:"soc"`
	Timestamp time.Time `json:"timestamp"`
}

type SegmentState struct {
	Start  Coords `json:"start"`
	Latest Coords `json:"current"`
	Active bool   `json:"active"`
}

type Coords struct {
	Time      time.Time `json:"time"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
}

type SegmentEvent struct {
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	DeviceID string    `json:"deviceID"`
}

func New(log *zerolog.Logger, s *config.Settings) *SegmentProcessor {
	kc := sarama.NewConfig()
	kc.Producer.Return.Successes = true
	p, err := sarama.NewSyncProducer(strings.Split(s.KafkaBrokers, ","), kc)
	if err != nil {
		panic(err)
	}

	return &SegmentProcessor{
		segments:    make(map[string]SegmentState),
		logger:      log,
		gracePeriod: tripGracePeriod,
		producer:    p,
		topic:       s.TripEventTopic,
	}
}

func (sp *SegmentProcessor) MovementDetected(p1 haversine.Coord, p2 haversine.Coord) bool {
	_, km := haversine.Distance(p1, p2)
	return km > 0.01 // estimate of point-to-point drift (10meters,~33feet)
}

func (sp *SegmentProcessor) Process(_ context.Context, event *shared.CloudEvent[PartialStatusData]) error {
	var segState SegmentState
	var ok bool
	segState, ok = sp.segments[event.Subject]
	if !ok {
		segState.Latest.Latitude = event.Data.Latitude
		segState.Latest.Longitude = event.Data.Longitude
		segState.Latest.Time = event.Data.Timestamp
		sp.segments[event.Subject] = segState
		return nil
	}

	if !segState.Active && sp.MovementDetected(
		haversine.Coord{Lat: segState.Latest.Latitude, Lon: segState.Latest.Longitude},
		haversine.Coord{Lat: event.Data.Latitude, Lon: event.Data.Longitude},
	) {
		segState.Start.Latitude = segState.Latest.Latitude
		segState.Start.Longitude = segState.Latest.Longitude
		segState.Start.Time = segState.Latest.Time
		segState.Latest.Latitude = event.Data.Latitude
		segState.Latest.Longitude = event.Data.Longitude
		segState.Latest.Time = event.Data.Timestamp
		segState.Active = true
		sp.segments[event.Subject] = segState
		return nil
	}

	if segState.Active && event.Data.Timestamp.Sub(segState.Latest.Time) > sp.gracePeriod {
		mi, _ := haversine.Distance(
			haversine.Coord{Lat: segState.Latest.Latitude, Lon: segState.Latest.Longitude},
			haversine.Coord{Lat: segState.Start.Latitude, Lon: segState.Start.Longitude},
		)
		if mi > 0.01 {
			completedSeg := SegmentEvent{
				Start:    segState.Start.Time,
				End:      segState.Latest.Time,
				DeviceID: event.Subject,
			}
			b, err := json.Marshal(completedSeg)
			if err != nil {
				return err
			}
			_, _, err = sp.producer.SendMessage(&sarama.ProducerMessage{
				Topic: sp.topic,
				Key:   sarama.StringEncoder(event.Subject),
				Value: sarama.ByteEncoder(b),
			})
			if err != nil {
				return err
			}
		}
		delete(sp.segments, event.Subject)
		return nil
	}

	segState.Latest.Latitude = event.Data.Latitude
	segState.Latest.Longitude = event.Data.Longitude
	segState.Latest.Time = event.Data.Timestamp
	sp.segments[event.Subject] = segState

	return nil
}
