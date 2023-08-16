package services

import (
	"context"
	"fmt"
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

func NewSegmenter(log *zerolog.Logger, s *config.Settings) *SegmentProcessor {
	return &SegmentProcessor{
		segments:    make(map[string]SegmentState),
		logger:      log,
		gracePeriod: tripGracePeriod,
	}
}

func (sp *SegmentProcessor) MovementDetected(p1 haversine.Coord, p2 haversine.Coord) bool {
	_, km := haversine.Distance(p1, p2)
	if km > 0 { // do we want to include a threshold here for gps drift?
		return true
	}
	return false
}

func (sp *SegmentProcessor) Process(ctx context.Context, event *shared.CloudEvent[PartialStatusData]) error {
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
		fmt.Println("\nStart a trip")
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
			fmt.Printf("\tTrip Details:\tStart: %+v End: %+v Distance Traveled: %+v\n", segState.Start.Time, segState.Latest.Time, mi)
			fmt.Println("Trip Ends\n")
		}
		delete(sp.segments, event.Subject)
		return nil
	}

	segState.Latest.Latitude = event.Data.Latitude
	segState.Latest.Longitude = event.Data.Longitude
	segState.Latest.Time = event.Data.Timestamp
	sp.segments[event.Subject] = segState

	fmt.Printf("\t\tUpdating Record With:\tTime: %+v\tLatitude %+v\tLongitude %+v\n", segState.Latest.Time, segState.Latest.Latitude, segState.Latest.Longitude)

	return nil
}
