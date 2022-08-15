package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/tidwall/gjson"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "new-topic"
	group   goka.Group  = "mini-group"

	tmc *goka.TopicManagerConfig
)

type coordinates struct {
	Latitude  float64 `json:"Latitude"`
	Longitude float64 `json:"Longitude"`
	Timestamp string  `json:"Start"`
}

// A trip is the object that is stored in the processor's group table
type trip struct {
	Key         string    `json:"Key"`
	Start       string    `json:"Start"`
	LatestTime  time.Time `json:"LatestTime"`
	LatestSpeed float64   `json:"LatestSpeed"`
	Route       []coordinates
	// AutoExpireTrip *time.Timer
}

// This codec allows marshalling (encode) and unmarshalling (decode) the trip to and from the
// group table.
type tripCodec struct{}

func init() {
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}

// Encodes a trip into []byte
func (jc *tripCodec) Encode(value interface{}) ([]byte, error) {
	if _, istrip := value.(*trip); !istrip {
		return nil, fmt.Errorf("Codec requires value *trip, got %T", value)
	}
	return json.Marshal(value)
}

// Decodes a trip from []byte to it's go representation.
func (jc *tripCodec) Decode(data []byte) (interface{}, error) {
	var (
		c   trip
		err error
	)
	err = json.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling trip: %v", err)
	}
	return &c, nil
}

func runDataEmitter() {
	emitter, err := goka.NewEmitter(brokers, topic,
		new(codec.String))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	data := loadData("producer/teslaMulti.json")

	for key, element := range data {
		emitter.EmitSync(key, element)
	}
}

func runCompletedTripsEmitter() {
	emitter, err := goka.NewEmitter(brokers, "completed-trips",
		new(codec.String))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()
}

func ingestRecord(record interface{}) (*trip, error) {
	var deviceData *trip
	var coords coordinates
	var err error
	json.Unmarshal([]byte(record.(string)), &deviceData)
	json.Unmarshal([]byte(record.(string)), &coords)

	deviceData.LatestTime, err = time.Parse("2006-01-02T15:04:05", deviceData.Start)
	if err != nil {
		return &trip{}, nil
	}

	deviceData.Route = append(deviceData.Route, coords)
	return deviceData, nil
}

func process(ctx goka.Context, deviceData interface{}) {

	devicePointInTime, err := ingestRecord(deviceData)
	if err != nil {
		fmt.Println(err)
	}

	if val := ctx.Value(); val != nil {
		existingRecord := val.(*trip)
		if devicePointInTime.LatestSpeed > 0 {
			if devicePointInTime.LatestTime.Sub(existingRecord.LatestTime).Minutes() < 15 {
				existingRecord.Route = append(existingRecord.Route, devicePointInTime.Route[0])
				existingRecord.LatestTime = devicePointInTime.LatestTime
				ctx.Delete()
				ctx.SetValue(existingRecord)
				// fmt.Println("Record Updated: ", existingRecord.Key, existingRecord.Route)
			} else {
				ctx.Emit("completed-trips", ctx.Key(), existingRecord)
				psqlInfo := fmt.Sprintf(
					"host=localhost port=5433 user=postgres password=postgres dbname=pg_db sslmode=disable",
				)
				db, err := sql.Open("postgres", psqlInfo)
				if err != nil {
					panic(err)
				}
				err = db.Ping()
				if err != nil {
					panic(err)
				}

				for n := 0; n < len(existingRecord.Route); n++ {

					geometry := fmt.Sprintf("POINT(%f %f)", existingRecord.Route[n].Longitude, existingRecord.Route[n].Latitude)
					if existingRecord.Route[n].Longitude != 0.0 && existingRecord.Route[n].Latitude != 0.0 {
						query := `INSERT INTO trips (devicekey, geom, pointnum, coord_timestamp) VALUES ($1, $2, $3, $4)`
						_, err := db.Exec(query, ctx.Key(), geometry, n, existingRecord.Route[n].Timestamp)
						if err != nil {
							fmt.Println(err)
						}
					} else {
						fmt.Println(devicePointInTime.Route)
					}
				}
				ctx.Delete()
				ctx.SetValue(devicePointInTime)
				// fmt.Println("New Record Created: ", newRecord.Key, newRecord.Route, ctx.Partition())
			}
		} else {
			// fmt.Println("Speed not above 0")
		}
	} else {
		ctx.SetValue(devicePointInTime)
		// fmt.Println("New Record Created: ", newRecord.Key, newRecord.Route, ctx.Partition())
	}
}

func runProcessor(initialized chan struct{}) {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), process),
		goka.Persist(new(tripCodec)),
		goka.Output("completed-trips", new(tripCodec)),
	)
	p, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
	)
	if err != nil {
		panic(err)
	}

	close(initialized)

	p.Run(context.Background())
}

func runView(initialized chan struct{}) {
	<-initialized

	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(tripCodec),
	)
	if err != nil {
		panic(err)
	}

	root := mux.NewRouter()
	root.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {
		// var tripRecord *trip
		value, _ := view.Get(mux.Vars(r)["key"])
		data, _ := json.Marshal(value)
		// json.Unmarshal(data, &tripRecord)
		// result, _ := json.Marshal(tripRecord.Route)
		w.Write(data)
	})
	fmt.Println("View opened at http://localhost:9095/")
	go http.ListenAndServe(":9095", root)

	view.Run(context.Background())
}

func loadData(path string) map[string]string {
	jsonFile, err := os.Open("teslaMulti.json")
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	numResponses := gjson.Get(string(byteValue), "hits.hits.#").Int()

	data := make(map[string]string, numResponses)
	for i := 0; int64(i) < numResponses/2; i++ {
		base := fmt.Sprintf("hits.hits.%d._source", i)
		key := gjson.Get(string(byteValue), base+".subject").Str
		speed := gjson.Get(string(byteValue), base+".data.speed").Float()
		lat := gjson.Get(string(byteValue), base+".data.latitude").Float()
		lon := gjson.Get(string(byteValue), base+".data.longitude").Float()
		timestamp := strings.Replace(gjson.Get(string(byteValue), base+".data.timestamp").Str, "Z", "", 1)
		message := fmt.Sprintf(`{"Key": "%s", "LatestSpeed": %f, "Start": "%s", "Latitude": %f, "Longitude": %f}`, key, speed, timestamp, lat, lon)
		data[key] = message
	}
	return data
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
	// When this example is run the first time, wait for creation of all internal topics (this is done
	// by goka.NewProcessor)
	initialized := make(chan struct{})

	// go runEmitter()
	go runCompletedTripsEmitter()
	go runProcessor(initialized)
	runView(initialized)
}
