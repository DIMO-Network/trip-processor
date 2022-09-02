package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers                = []string{"localhost:9092"}
	topic      goka.Stream = "topic.device.status"
	newRecords goka.Stream = "new_record"
	group      goka.Group  = "mini-group"

	tmc *goka.TopicManagerConfig

	pgController PostgresController
)

type Data struct {
	Data struct {
		ChargeLimit float64   `json:"chargeLimit,omitempty"`
		Odometer    float64   `json:"odometer,omitempty"`
		Year        int       `json:"year,omitempty"`
		Soc         float64   `json:"soc,omitempty"`
		Latitude    float64   `json:"latitude,omitempty"`
		Charging    bool      `json:"charging,omitempty"`
		Range       float64   `json:"range,omitempty"`
		Speed       float64   `json:"speed,omitempty"`
		Model       string    `json:"model,omitempty"`
		VehicleID   string    `json:"vehicleId,omitempty"`
		Make        string    `json:"make,omitempty"`
		Longitude   float64   `json:"longitude,omitempty"`
		Timestamp   time.Time `json:"timestamp,omitempty"`
		LatestTime  time.Time
		Start       time.Time
	} `json:"data,omitempty"`
	Route []tripPointinTime
}

type tripPointinTime struct {
	Latitude  float64 `json:"latitude,omitempty"`
	Longitude float64 `json:"longitude,omitempty"`
	Timestamp time.Time
	Speed     float64
}

type DataCodec struct{}

type DeviceTrip struct {
	Id         string
	Start      time.Time
	End        time.Time
	LastActive time.Time
	LastIdle   time.Time
}

type PostgresController struct {
	db *sql.DB
}

func init() {
	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1

	psqlInfo := fmt.Sprintf(
		"host=localhost port=5433 user=postgres password=postgres dbname=pg_db sslmode=disable",
	)
	var err error
	pgController.db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	err = pgController.db.Ping()
	if err != nil {
		panic(err)
	}
}

func (pgc *PostgresController) StoreTrip(trp DeviceTrip) error {

	query := `INSERT INTO fulltrips (id, tripstart, tripend) VALUES ($1, $2, $3) `
	_, err := pgc.db.Exec(query, trp.Id, trp.Start, trp.End)
	if err != nil {
		return err
	}
	return nil
}

// Encodes a trip into []byte
func (jc *DataCodec) Encode(value interface{}) ([]byte, error) {
	if _, istrip := value.(*Data); !istrip {
		return nil, fmt.Errorf("Codec requires value *data, got %T", value)
	}
	return json.Marshal(value)
}

// Decodes a trip from []byte to it's go representation.
func (jc *DataCodec) Decode(data []byte) (interface{}, error) {
	var decoded Data
	err := json.Unmarshal(data, &decoded)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling data: %v", err)
	}
	return &decoded, nil
}

type newRecordCodec struct{}

func (c *newRecordCodec) Encode(value interface{}) ([]byte, error) {
	if _, istrip := value.(*DeviceTrip); !istrip {
		return nil, fmt.Errorf("new record codec requires value *data, got %T", value)
	}
	return json.Marshal(value)
}

func (c *newRecordCodec) Decode(data []byte) (interface{}, error) {
	var decoded DeviceTrip
	err := json.Unmarshal(data, &decoded)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling new record data: %v", err)
	}
	return &decoded, nil
}

// Emit messages forever every second
func runEmitter() {
	emitter, err := goka.NewEmitter(brokers, topic, new(codec.String))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish()
	for {
		time.Sleep(1 * time.Second)
		err = emitter.EmitSync("some-key", "some-value")
		if err != nil {
			log.Fatalf("error emitting message: %v", err)
		}
	}
}

// process messages until ctrl-c is pressed
func runProcessor() {
	cb := func(ctx goka.Context, msg interface{}) {

		var existingRecord *DeviceTrip
		newRecord := msg.(*Data)

		if val := ctx.Value(); val != nil {
			existingRecord = val.(*DeviceTrip)

			if newRecord.Data.Timestamp.Sub(existingRecord.LastActive).Minutes() <= 15 {
				if newRecord.Data.Speed > 0 {
					existingRecord.LastActive = newRecord.Data.Timestamp
					ctx.SetValue(existingRecord)
				}
			} else {
				if existingRecord.Start != existingRecord.LastActive {
					existingRecord.End = existingRecord.LastActive
					err := pgController.StoreTrip(*existingRecord)
					if err != nil {
						log.Fatal(err)
					}
					ctx.Delete()
				}
				if newRecord.Data.Speed > 0 {
					beginTrip := new(DeviceTrip)
					beginTrip.Id = ctx.Key()
					beginTrip.Start = newRecord.Data.Timestamp.UTC()
					beginTrip.LastActive = beginTrip.Start
					ctx.SetValue(beginTrip)
				}
			}

		} else {
			beginTrip := new(DeviceTrip)
			beginTrip.Id = ctx.Key()
			beginTrip.Start = newRecord.Data.Timestamp.UTC()
			beginTrip.LastActive = beginTrip.Start
			ctx.SetValue(beginTrip)
		}
	}

	// Define a new processor group. The group defines all inputs, outputs, and
	// serialization formats. The group-table topic is "example-group-table".
	g := goka.DefineGroup(group,
		goka.Input(topic, new(DataCodec), cb),
		goka.Persist(new(newRecordCodec)),
	)

	p, err := goka.NewProcessor(brokers, g,
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder))
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			log.Fatalf("error running processor: %v", err)
		} else {
			log.Printf("Processor shutdown cleanly")
		}
	}()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait   // wait for SIGINT/SIGTERM
	cancel() // gracefully stop processor
	<-done
}

func main() {
	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	defer tm.Close()
	err = tm.EnsureStreamExists(string(topic), 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", topic, err)
	}

	runProcessor() // press ctrl-c to stop
}
