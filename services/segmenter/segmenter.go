package segmenter

import (
	"time"

	"github.com/DIMO-Network/shared"
	"github.com/DIMO-Network/trips-api/internal/config"
	"github.com/lovoo/goka"
	"github.com/rs/zerolog"
)

type SegmentProcessor struct {
	logger                *zerolog.Logger
	GracePeriod           time.Duration
	CompletedSegmentTopic goka.Stream
}

type PartialStatusData struct {
	Latitude  *float64  `json:"latitude"`
	Longitude *float64  `json:"longitude"`
	Timestamp time.Time `json:"timestamp"`
}

type State struct {
	ActiveSegment *Segment  `json:"activeSegment,omitempty"`
	Latest        PointTime `json:"latest"`
}

type Segment struct {
	Start        PointTime `json:"start"`
	LastMovement PointTime `json:"lastMovement"`
}

type Point struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type PointTime struct {
	Point Point     `json:"point"`
	Time  time.Time `json:"time"`
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

func (sp *SegmentProcessor) MovementDetected(p1 Point, p2 Point) bool {
	return Distance(p1, p2) > 10 // estimate of point-to-point drift (10meters,~33feet)
}

func (sp *SegmentProcessor) Process(ctx goka.Context, msg any) {
	userDeviceID := ctx.Key()
	newDeviceStatus := msg.(*shared.CloudEvent[PartialStatusData])

	logger := sp.logger.With().Str("userDeviceId", userDeviceID).Time("eventTime", newDeviceStatus.Data.Timestamp).Logger()

	if newDeviceStatus.Source != "dimo/integration/27qftVRWQYpVDcO5DltO5Ojbjxk" &&
		newDeviceStatus.Source != "dimo/integration/26A5Dk3vvvQutjSyF0Jka2DP5lg" ||
		newDeviceStatus.Data.Latitude == nil ||
		newDeviceStatus.Data.Longitude == nil {
		return
	}

	newPoint := Point{
		Latitude:  *newDeviceStatus.Data.Latitude,
		Longitude: *newDeviceStatus.Data.Longitude,
	}

	newPointTime := PointTime{
		Point: newPoint,
		Time:  newDeviceStatus.Data.Timestamp,
	}

	var state *State

	val := ctx.Value()
	if val == nil {
		logger.Debug().Msg("New vehicle.")
		ctx.SetValue(&State{Latest: newPointTime})
		return
	}

	state = val.(*State)

	dist := Distance(state.Latest.Point, newPoint)

	if state.ActiveSegment == nil {
		if dist >= 10 {
			logger.Debug().Msgf("Moved %fm, starting a segment.", dist)
			state.ActiveSegment = &Segment{
				Start:        newPointTime,
				LastMovement: newPointTime,
			}
		}
	} else {
		if dist < 10 {
			if idle := newPointTime.Time.Sub(state.ActiveSegment.LastMovement.Time); idle >= sp.GracePeriod {
				logger.Debug().Msgf("Last significant movement was %s ago, ending segment.", idle)
				event := SegmentEvent{
					Start:    state.ActiveSegment.Start.Time,
					End:      state.ActiveSegment.LastMovement.Time,
					DeviceID: userDeviceID,
				}

				ctx.Emit(sp.CompletedSegmentTopic, userDeviceID, event)

				state.ActiveSegment = nil
			} else {
				logger.Debug().Msgf("Only moved %fm. Last significant movement was %s ago.", dist, idle)
			}
		} else {
			logger.Debug().Msgf("Moved %fm. Continuing segment.", dist)
			state.ActiveSegment.LastMovement = newPointTime
		}
	}

	state.Latest = newPointTime
	ctx.SetValue(state)
}
