package segmenter

import (
	"fmt"
	"time"

	"github.com/DIMO-Network/shared"
	"github.com/DIMO-Network/trips-api/internal/config"
	"github.com/DIMO-Network/trips-api/services/haversine"
	"github.com/lovoo/goka"
	"github.com/rs/zerolog"
)

type SegmentProcessor struct {
	logger                *zerolog.Logger
	GracePeriod           time.Duration
	CompletedSegmentTopic goka.Stream
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

func New(log *zerolog.Logger, gp time.Duration, s *config.Settings) *SegmentProcessor {
	return &SegmentProcessor{
		logger:                log,
		GracePeriod:           gp,
		CompletedSegmentTopic: goka.Stream(s.TripEventTopic),
	}
}

func (sp *SegmentProcessor) MovementDetected(p1 haversine.Coord, p2 haversine.Coord) bool {
	_, km := haversine.Distance(p1, p2)
	return km > 0.01 // estimate of point-to-point drift (10meters,~33feet)
}

func (sp *SegmentProcessor) Process(ctx goka.Context, msg any) {
	userDeviceID := ctx.Key()
	newDeviceStatus := msg.(*shared.CloudEvent[PartialStatusData])

	if val := ctx.Value(); val != nil {
		fmt.Println(val)
		event := val.(*SegmentState)

		if !event.Active && sp.MovementDetected(
			haversine.Coord{Lat: event.Latest.Latitude, Lon: event.Latest.Longitude},
			haversine.Coord{Lat: newDeviceStatus.Data.Latitude, Lon: newDeviceStatus.Data.Longitude},
		) {
			event.Start.Latitude = event.Latest.Latitude
			event.Start.Longitude = event.Latest.Longitude
			event.Start.Time = event.Latest.Time
			event.Latest.Latitude = newDeviceStatus.Data.Latitude
			event.Latest.Longitude = newDeviceStatus.Data.Longitude
			event.Latest.Time = newDeviceStatus.Data.Timestamp
			event.Active = true
			ctx.SetValue(event)
			return
		}

		if event.Active && newDeviceStatus.Data.Timestamp.Sub(event.Latest.Time) > sp.GracePeriod {
			completedSeg := SegmentEvent{
				Start:    event.Start.Time,
				End:      event.Latest.Time,
				DeviceID: userDeviceID,
			}

			ctx.Emit(sp.CompletedSegmentTopic, userDeviceID, completedSeg)
			ctx.Delete()
			return
		}

		event.Latest.Latitude = newDeviceStatus.Data.Latitude
		event.Latest.Longitude = newDeviceStatus.Data.Longitude
		event.Latest.Time = newDeviceStatus.Data.Timestamp
		ctx.SetValue(event)
		return
	}

	var s SegmentState
	s.Latest.Latitude = newDeviceStatus.Data.Latitude
	s.Latest.Longitude = newDeviceStatus.Data.Longitude
	s.Latest.Time = newDeviceStatus.Data.Timestamp
	ctx.SetValue(s)
	return
}
