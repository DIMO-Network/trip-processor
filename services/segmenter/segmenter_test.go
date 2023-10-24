package segmenter

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/shared"
	"github.com/DIMO-Network/trips-api/internal/config"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/tester"
	"github.com/rs/zerolog"
	"github.com/smallstep/assert"
	"github.com/stretchr/testify/require"
)

var deviceStatusCodec = &shared.JSONCodec[shared.CloudEvent[PartialStatusData]]{}
var segmentStateCodec = &shared.JSONCodec[State]{}
var segmentEventCodec = &shared.JSONCodec[shared.CloudEvent[SegmentEvent]]{}

func TestSegmentProcessor_Process_StoreFirstObs_LatLon(t *testing.T) {
	logger := zerolog.New(nil)
	settings := config.Settings{
		ConsumerGroup:     "group",
		DeviceStatusTopic: "device-status-topic",
		TripEventTopic:    "trip-event-topic",
	}
	key := "7371a505-0c70-4bce-a116-1a3baa56a3e5"

	// Create a new test segment processor
	s := New(&logger, time.Second, &settings)
	gkt := tester.New(t)

	// create a new processor, registering the tester
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup(
		goka.Group(settings.ConsumerGroup),
		goka.Input(goka.Stream(settings.DeviceStatusTopic), deviceStatusCodec, s.Process),
		goka.Persist(segmentStateCodec),
		goka.Output(goka.Stream(settings.TripEventTopic), segmentEventCodec),
	),
		goka.WithTester(gkt),
	)

	go func() {
		err := proc.Run(context.Background())
		assert.NoError(t, err)
	}()

	msg1 := shared.CloudEvent[PartialStatusData]{
		ID: key,
		Data: PartialStatusData{
			Latitude:  float64Ptr(39.7532),
			Longitude: float64Ptr(-105.02166),
			Timestamp: time.Date(2023, time.October, 1, 2, 3, 4, 5, time.UTC),
		},
		Source: "dimo/integration/26A5Dk3vvvQutjSyF0Jka2DP5lg",
	}

	var tblResult State
	tblResult.Latest = PointTime{
		Point: Point{
			Latitude:  *msg1.Data.Latitude,
			Longitude: *msg1.Data.Longitude,
		},
		Time: msg1.Data.Timestamp,
	}

	gkt.Consume(settings.DeviceStatusTopic, key, msg1)

	v := gkt.TableValue("group-table", key)
	value := v.(*State)
	require.Equal(t, tblResult.Latest, value.Latest)
}

func TestSegmentProcessor_Process_StoreActiveSeg_LatLon(t *testing.T) {
	logger := zerolog.New(nil)
	settings := config.Settings{
		ConsumerGroup:     "group",
		DeviceStatusTopic: "device-status-topic",
		TripEventTopic:    "trip-event-topic",
	}
	key := "7371a505-0c70-4bce-a116-1a3baa56a3e5"

	// Create a new test segment processor
	s := New(&logger, time.Second, &settings)
	gkt := tester.New(t)

	// create a new processor, registering the tester
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup(
		goka.Group(settings.ConsumerGroup),
		goka.Input(goka.Stream(settings.DeviceStatusTopic), deviceStatusCodec, s.Process),
		goka.Persist(segmentStateCodec),
		goka.Output(goka.Stream(settings.TripEventTopic), segmentEventCodec),
	),
		goka.WithTester(gkt),
	)

	go func() {
		err := proc.Run(context.Background())
		assert.NoError(t, err)
	}()

	msg1 := shared.CloudEvent[PartialStatusData]{
		ID: key,
		Data: PartialStatusData{
			Latitude:  float64Ptr(39.7532),
			Longitude: float64Ptr(-105.02166),
			Timestamp: time.Date(2023, time.October, 1, 2, 3, 4, 5, time.UTC),
		},
		Source: "dimo/integration/26A5Dk3vvvQutjSyF0Jka2DP5lg",
	}

	msg2 := shared.CloudEvent[PartialStatusData]{
		ID: key,
		Data: PartialStatusData{
			Latitude:  float64Ptr(39.8),
			Longitude: float64Ptr(-105.2),
			Timestamp: time.Date(2023, time.October, 1, 2, 4, 4, 5, time.UTC),
		},
		Source: "dimo/integration/26A5Dk3vvvQutjSyF0Jka2DP5lg",
	}

	var tblResult State
	tblResult.ActiveSegment = &Segment{
		Start: PointTime{
			Point: Point{
				Latitude:  *msg1.Data.Latitude,
				Longitude: *msg1.Data.Longitude,
			},
			Time: msg1.Data.Timestamp,
		},
		LastMovement: PointTime{
			Point: Point{
				Latitude:  *msg2.Data.Latitude,
				Longitude: *msg2.Data.Longitude,
			},
			Time: msg2.Data.Timestamp,
		},
	}

	gkt.Consume(settings.DeviceStatusTopic, key, msg1)
	gkt.Consume(settings.DeviceStatusTopic, key, msg2)

	v := gkt.TableValue("group-table", key)
	value := v.(*State)
	require.Equal(t, tblResult.ActiveSegment.Start, value.ActiveSegment.Start)
	require.Equal(t, tblResult.ActiveSegment.LastMovement, value.ActiveSegment.LastMovement)
}

func TestSegmentProcessor_Process_StoreFirstObs_Speed(t *testing.T) {
	logger := zerolog.New(nil)
	settings := config.Settings{
		ConsumerGroup:     "group",
		DeviceStatusTopic: "device-status-topic",
		TripEventTopic:    "trip-event-topic",
	}
	key := "7371a505-0c70-4bce-a116-1a3baa56a3e5"

	// Create a new test segment processor
	s := New(&logger, time.Second, &settings)
	gkt := tester.New(t)

	// create a new processor, registering the tester
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup(
		goka.Group(settings.ConsumerGroup),
		goka.Input(goka.Stream(settings.DeviceStatusTopic), deviceStatusCodec, s.Process),
		goka.Persist(segmentStateCodec),
		goka.Output(goka.Stream(settings.TripEventTopic), segmentEventCodec),
	),
		goka.WithTester(gkt),
	)

	go func() {
		err := proc.Run(context.Background())
		assert.NoError(t, err)
	}()

	speed := 40.0
	msg1 := shared.CloudEvent[PartialStatusData]{
		ID: key,
		Data: PartialStatusData{
			Latitude:  float64Ptr(39.8),
			Longitude: float64Ptr(-105.2),
			Speed:     &speed,
			Timestamp: time.Date(2023, time.October, 1, 2, 3, 4, 5, time.UTC),
		},
		Source: "dimo/integration/26A5Dk3vvvQutjSyF0Jka2DP5lg",
	}

	var tblResult State
	tblResult.Latest = PointTime{
		Point: Point{
			Latitude:  *msg1.Data.Latitude,
			Longitude: *msg1.Data.Longitude,
		},
		Time: msg1.Data.Timestamp,
	}

	gkt.Consume(settings.DeviceStatusTopic, key, msg1)

	v := gkt.TableValue("group-table", key)
	value := v.(*State)
	require.Equal(t, tblResult.Latest, value.Latest)
}

func TestSegmentProcessor_Process_StoreActiveSeg_Speed(t *testing.T) {
	logger := zerolog.New(nil)
	settings := config.Settings{
		ConsumerGroup:     "group",
		DeviceStatusTopic: "device-status-topic",
		TripEventTopic:    "trip-event-topic",
	}
	key := "7371a505-0c70-4bce-a116-1a3baa56a3e5"

	// Create a new test segment processor
	s := New(&logger, time.Second, &settings)
	gkt := tester.New(t)

	// create a new processor, registering the tester
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup(
		goka.Group(settings.ConsumerGroup),
		goka.Input(goka.Stream(settings.DeviceStatusTopic), deviceStatusCodec, s.Process),
		goka.Persist(segmentStateCodec),
		goka.Output(goka.Stream(settings.TripEventTopic), segmentEventCodec),
	),
		goka.WithTester(gkt),
	)

	go func() {
		err := proc.Run(context.Background())
		assert.NoError(t, err)
	}()

	speed := 40.0
	msg1 := shared.CloudEvent[PartialStatusData]{
		ID: key,
		Data: PartialStatusData{
			Latitude:  float64Ptr(39.8),
			Longitude: float64Ptr(-105.2),
			Speed:     &speed,
			Timestamp: time.Date(2023, time.October, 1, 2, 3, 4, 5, time.UTC),
		},
		Source: "dimo/integration/26A5Dk3vvvQutjSyF0Jka2DP5lg",
	}

	speed += 20
	msg2 := shared.CloudEvent[PartialStatusData]{
		ID: key,
		Data: PartialStatusData{
			Latitude:  float64Ptr(39.8),
			Longitude: float64Ptr(-105.2),
			Speed:     &speed,
			Timestamp: time.Date(2023, time.October, 1, 2, 4, 4, 5, time.UTC),
		},
		Source: "dimo/integration/26A5Dk3vvvQutjSyF0Jka2DP5lg",
	}

	var tblResult State
	tblResult.ActiveSegment = &Segment{
		Start: PointTime{
			Point: Point{
				Latitude:  *msg1.Data.Latitude,
				Longitude: *msg1.Data.Longitude,
			},
			Time: msg1.Data.Timestamp,
		},
		LastMovement: PointTime{
			Point: Point{
				Latitude:  *msg2.Data.Latitude,
				Longitude: *msg2.Data.Longitude,
			},
			Time: msg2.Data.Timestamp,
		},
	}

	gkt.Consume(settings.DeviceStatusTopic, key, msg1)
	gkt.Consume(settings.DeviceStatusTopic, key, msg2)

	v := gkt.TableValue("group-table", key)
	value := v.(*State)
	require.Equal(t, tblResult.ActiveSegment.LastMovement, value.ActiveSegment.LastMovement)
	require.Equal(t, tblResult.ActiveSegment.Start, value.ActiveSegment.Start)
}

func float64Ptr(f float64) *float64 {
	return &f
}
