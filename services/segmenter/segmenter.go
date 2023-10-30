package segmenter

import (
	"time"

	"github.com/DIMO-Network/shared"
	"github.com/DIMO-Network/trips-api/internal/config"
	"github.com/lovoo/goka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/segmentio/ksuid"
)

type SegmentProcessor struct {
	logger            *zerolog.Logger
	GracePeriod       time.Duration
	SegmentEventTopic goka.Stream
}

type PartialStatusData struct {
	Latitude  *float64  `json:"latitude"`
	Longitude *float64  `json:"longitude"`
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
	ID           string    `json:"id"`
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
	Start     PointTime `json:"start"`
	End       PointTime `json:"end"`
	DeviceID  string    `json:"deviceId"`
	Completed bool      `json:"completed"`
	ID        string    `json:"id"`
}

func New(log *zerolog.Logger, gp time.Duration, s *config.Settings) *SegmentProcessor {
	return &SegmentProcessor{
		logger:            log,
		GracePeriod:       gp,
		SegmentEventTopic: goka.Stream(s.TripEventTopic),
	}
}

// SpeedCalc returns an estimate of the average speed traveling from PointTime p1 to
// PointTime p2. The return value is in km/h.
func SpeedCalc(p1 PointTime, p2 PointTime) float64 {
	dist := Distance(p1.Point, p2.Point)
	dur := p2.Time.Sub(p1.Time)
	return dist / dur.Hours()
}

// speedThreshold is the speed, in km/h, that we consider to be showing movement and
// not merely GPS noise.
const speedThreshold = 15

func (sp *SegmentProcessor) Process(ctx goka.Context, msg any) {
	userDeviceID := ctx.Key()
	newDeviceStatus := msg.(*shared.CloudEvent[PartialStatusData])

	logger := sp.logger.With().Str("userDeviceId", userDeviceID).Time("eventTime", newDeviceStatus.Data.Timestamp).Logger()

	if newDeviceStatus.Source != "dimo/integration/27qftVRWQYpVDcO5DltO5Ojbjxk" &&
		newDeviceStatus.Source != "dimo/integration/26A5Dk3vvvQutjSyF0Jka2DP5lg" ||
		newDeviceStatus.Data.Latitude == nil || newDeviceStatus.Data.Longitude == nil {
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

	// Speed, perhaps an estimate, in km/h.
	var speed float64

	if newDeviceStatus.Data.Speed != nil {
		speed = *newDeviceStatus.Data.Speed
	} else {
		speed = SpeedCalc(state.Latest, newPointTime)
	}

	if state.ActiveSegment == nil {
		if speed >= speedThreshold {
			logger.Debug().Msgf("Moving at %f m/s, starting a segment.", speed)
			state.ActiveSegment = &Segment{
				Start:        state.Latest,
				LastMovement: newPointTime,
				ID:           ksuid.New().String(),
			}

			event := shared.CloudEvent[SegmentEvent]{
				Data: SegmentEvent{
					Start:    state.ActiveSegment.Start,
					DeviceID: userDeviceID,
					ID:       state.ActiveSegment.ID,
				},
			}
			ctx.Emit(sp.SegmentEventTopic, userDeviceID, event)
			OngoingSegmentsTotal.Inc()
		}
	} else {
		if speed < speedThreshold {
			if idle := newPointTime.Time.Sub(state.ActiveSegment.LastMovement.Time); idle >= sp.GracePeriod {
				logger.Debug().Msgf("Last significant movement was %s ago, ending segment.", idle)
				event := shared.CloudEvent[SegmentEvent]{
					Data: SegmentEvent{
						Start:     state.ActiveSegment.Start,
						End:       state.ActiveSegment.LastMovement,
						DeviceID:  userDeviceID,
						Completed: true,
						ID:        state.ActiveSegment.ID,
					},
				}

				ctx.Emit(sp.SegmentEventTopic, userDeviceID, event)
				CompletedSegmentsTotal.Inc()
				OngoingSegmentsTotal.Dec()

				state.ActiveSegment = nil
			} else {
				logger.Debug().Msgf("Moving at %f m/s. Last significant movement was %s ago.", speed, idle)
			}
		} else {
			logger.Debug().Msgf("Moving at %f m/s. Continuing segment.", speed)
			state.ActiveSegment.LastMovement = newPointTime
		}
	}

	state.Latest = newPointTime
	ctx.SetValue(state)
}

var (
	CompletedSegmentsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "trip_processor",
			Name:      "completed_segments_total",
			Help:      "The total number of completed trip segments.",
		},
	)

	OngoingSegmentsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "trip_processor",
			Name:      "ongoing_segments_total",
			Help:      "The total number of ongoing trip segments.",
		},
	)
)
