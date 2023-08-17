package segmenter

import (
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
		state := val.(*SegmentState)

		if !state.Active && sp.MovementDetected(
			haversine.Coord{Lat: state.Latest.Latitude, Lon: state.Latest.Longitude},
			haversine.Coord{Lat: newDeviceStatus.Data.Latitude, Lon: newDeviceStatus.Data.Longitude},
		) {
			state.Start.Latitude = state.Latest.Latitude
			state.Start.Longitude = state.Latest.Longitude
			state.Start.Time = state.Latest.Time
			state.Latest.Latitude = newDeviceStatus.Data.Latitude
			state.Latest.Longitude = newDeviceStatus.Data.Longitude
			state.Latest.Time = newDeviceStatus.Data.Timestamp
			state.Active = true
			ctx.SetValue(state)
			return
		}

		if state.Active && newDeviceStatus.Data.Timestamp.Sub(state.Latest.Time) > sp.GracePeriod {
			event := SegmentEvent{
				Start:    state.Start.Time,
				End:      state.Latest.Time,
				DeviceID: userDeviceID,
			}

			ctx.Emit(sp.CompletedSegmentTopic, userDeviceID, event)
			ctx.Delete()
			return
		}
		state.Latest.Latitude = newDeviceStatus.Data.Latitude
		state.Latest.Longitude = newDeviceStatus.Data.Longitude
		state.Latest.Time = newDeviceStatus.Data.Timestamp
		ctx.SetValue(state)
		return
	}

	var s SegmentState
	s.Latest.Latitude = newDeviceStatus.Data.Latitude
	s.Latest.Longitude = newDeviceStatus.Data.Longitude
	s.Latest.Time = newDeviceStatus.Data.Timestamp
	ctx.SetValue(s)
}
