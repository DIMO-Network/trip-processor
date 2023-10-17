package segmenter

import (
	"time"

	"github.com/DIMO-Network/shared"
	"github.com/DIMO-Network/trips-api/internal/config"
	"github.com/lovoo/goka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
)

type SegmentProcessor struct {
	logger                *zerolog.Logger
	GracePeriod           time.Duration
	CompletedSegmentTopic goka.Stream
}

type Point struct {
	Latitude  *float64  `json:"latitude,omitempty"`
	Longitude *float64  `json:"longitude,omitempty"`
	Speed     *float64  `json:"speed,omitempty"`
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

type PointTime struct {
	Point Point     `json:"point"`
	Time  time.Time `json:"time"`
}

type SegmentEvent struct {
	Start    PointTime `json:"start"`
	End      PointTime `json:"end"`
	DeviceID string    `json:"deviceID"`
}

func New(log *zerolog.Logger, gp time.Duration, s *config.Settings) *SegmentProcessor {
	return &SegmentProcessor{
		logger:                log,
		GracePeriod:           gp,
		CompletedSegmentTopic: goka.Stream(s.TripEventTopic),
	}
}

func (sp *SegmentProcessor) SpeedCalc(p1 Point, p2 Point) float64 {
	if p2.Speed != nil {
		return *p2.Speed * convertToMetersPerSec
	}

	if p1.Latitude != nil && p1.Longitude != nil && p2.Latitude != nil && p2.Longitude != nil {
		dist := Distance(p1, p2)
		dur := p2.Timestamp.Sub(p1.Timestamp)
		return dist / dur.Seconds()
	}
	return 0
}

// speedThreshold is the speed, in m/s, that we consider to be showing movement and
// not merely GPS noise. The number 5 here is roughly 11 mi/h.
const speedThreshold = 5
const convertToMetersPerSec = float64(5) / float64(18)

func (sp *SegmentProcessor) Process(ctx goka.Context, msg any) {
	userDeviceID := ctx.Key()
	newDeviceStatus := msg.(*shared.CloudEvent[Point])

	logger := sp.logger.With().Str("userDeviceId", userDeviceID).Time("eventTime", newDeviceStatus.Data.Timestamp).Logger()

	if newDeviceStatus.Source != "dimo/integration/27qftVRWQYpVDcO5DltO5Ojbjxk" &&
		newDeviceStatus.Source != "dimo/integration/26A5Dk3vvvQutjSyF0Jka2DP5lg" {
		return
	}

	if newDeviceStatus.Data.Speed == nil && (newDeviceStatus.Data.Latitude == nil || newDeviceStatus.Data.Longitude == nil) {
		return
	}

	newPointTime := PointTime{
		Point: newDeviceStatus.Data,
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
	estSpeed := sp.SpeedCalc(state.Latest.Point, newPointTime.Point)
	if state.ActiveSegment == nil {
		if estSpeed >= speedThreshold {
			logger.Debug().Msgf("Moving at %f m/s, starting a segment.", estSpeed)
			state.ActiveSegment = &Segment{
				Start:        newPointTime,
				LastMovement: newPointTime,
			}
		}
	} else {
		if estSpeed < speedThreshold {
			if idle := newPointTime.Time.Sub(state.ActiveSegment.LastMovement.Time); idle >= sp.GracePeriod {
				logger.Debug().Msgf("Last significant movement was %s ago, ending segment.", idle)
				event := shared.CloudEvent[SegmentEvent]{
					Data: SegmentEvent{
						Start:    state.ActiveSegment.Start,
						End:      state.ActiveSegment.LastMovement,
						DeviceID: userDeviceID,
					},
				}

				ctx.Emit(sp.CompletedSegmentTopic, userDeviceID, event)
				SegmentsEmittedTotal.Inc()

				state.ActiveSegment = nil
			} else {
				logger.Debug().Msgf("Moving at %f m/s. Last significant movement was %s ago.", estSpeed, idle)
			}
		} else {
			logger.Debug().Msgf("Moving at %f m/s. Continuing segment.", estSpeed)
			state.ActiveSegment.LastMovement = newPointTime
		}
	}

	state.Latest = newPointTime
	ctx.SetValue(state)
}

var (
	SegmentsEmittedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "trip_processor",
			Name:      "emitted_segments_total",
			Help:      "The total number of completed trip segments.",
		},
	)
)
