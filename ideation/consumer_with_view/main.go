package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
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
	Latitude  float64
	Longitude float64
}

// A user is the object that is stored in the processor's group table
type user struct {
	Key         string    `json:"Key"`
	Start       string    `json:"Start"`
	LatestTime  time.Time `json:"LatestTime"`
	LatestSpeed float64   `json:"LatestSpeed"`
	Route       []coordinates
}

// This codec allows marshalling (encode) and unmarshalling (decode) the user to and from the
// group table.
type userCodec struct{}

func init() {
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}

// Encodes a user into []byte
func (jc *userCodec) Encode(value interface{}) ([]byte, error) {
	if _, isUser := value.(*user); !isUser {
		return nil, fmt.Errorf("Codec requires value *user, got %T", value)
	}
	return json.Marshal(value)
}

// Decodes a user from []byte to it's go representation.
func (jc *userCodec) Decode(data []byte) (interface{}, error) {
	var (
		c   user
		err error
	)
	err = json.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling user: %v", err)
	}
	return &c, nil
}

func runEmitter() {
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

func process(ctx goka.Context, msg interface{}) {
	var newRecord *user
	var coords coordinates
	var err error
	json.Unmarshal([]byte(msg.(string)), &newRecord)
	json.Unmarshal([]byte(msg.(string)), &coords)

	newRecord.LatestTime, err = time.Parse("2006-01-02T15:04:05", newRecord.Start)
	if err != nil {
		fmt.Println(err)
	}

	if val := ctx.Value(); val != nil {
		existingRecord := val.(*user)
		if newRecord.LatestSpeed > 0 {
			if newRecord.LatestTime.Sub(existingRecord.LatestTime).Minutes() < 15 && newRecord.LatestTime.Sub(existingRecord.LatestTime).Minutes() > 15 {
				existingRecord.Route = append(existingRecord.Route, coords)
				existingRecord.LatestTime = newRecord.LatestTime
				ctx.Delete()
				ctx.SetValue(existingRecord)
				fmt.Println("Record Updated: ", existingRecord.Key, existingRecord.Route)
			} else {
				// fmt.Println("Deleted Record Due to Time Out")
				ctx.Delete()
			}
		} else {
			// fmt.Println("Speed not above 0")
		}
	} else {
		newRecord.Route = append(newRecord.Route, coords)
		ctx.SetValue(newRecord)
		fmt.Println("New Record Created: ", newRecord.Key, newRecord.Route, ctx.Partition())
	}
}

func runProcessor(initialized chan struct{}) {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), process),
		goka.Persist(new(userCodec)),
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
		new(userCodec),
	)
	if err != nil {
		panic(err)
	}

	root := mux.NewRouter()
	root.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {
		// var userRecord *user
		value, _ := view.Get(mux.Vars(r)["key"])
		data, _ := json.Marshal(value)
		// json.Unmarshal(data, &userRecord)
		// result, _ := json.Marshal(userRecord.Route)
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
	go runProcessor(initialized)
	runView(initialized)
}
