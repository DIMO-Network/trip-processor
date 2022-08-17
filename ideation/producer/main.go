package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/tidwall/gjson"
)

var topic string = "new-topic"
var partitions int32 = 9

func main() {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewHashPartitioner

	admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer admin.Close()

	tpcs, _ := admin.ListTopics()

	if _, ok := tpcs[topic]; !ok {
		err = admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			log.Fatal(err)
		}
	}

	client, err := sarama.NewClient([]string{"localhost:9092"}, config)
	defer client.Close()
	if err != nil {
		log.Fatal(err)
	}
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatal(err)
	}
	var successes, errors int

	docs := []string{"tesla_806_1.json", "tesla_806_2.json", "tesla_806_3.json", "tesla_806_4.json", "tesla_806_5.json"}
	// docs := []string{"teslaMulti.json"}
	// jsonFile, err := os.Open("tesla_816.json")
	for i := 0; i < len(docs); i++ {
		jsonFile, err := os.Open(docs[i])
		if err != nil {
			fmt.Println(err)
		}
		defer jsonFile.Close()
		byteValue, _ := ioutil.ReadAll(jsonFile)

		numResponses := gjson.Get(string(byteValue), "hits.hits.#").Int()

		for i := 0; int64(i) < numResponses; i++ {
			base := fmt.Sprintf("hits.hits.%d._source", i)

			key := gjson.Get(string(byteValue), base+".subject").Str
			speed := gjson.Get(string(byteValue), base+".data.speed").Float()
			lat := gjson.Get(string(byteValue), base+".data.latitude").Float()
			lon := gjson.Get(string(byteValue), base+".data.longitude").Float()
			odometer := gjson.Get(string(byteValue), base+".data.odometer").Float()
			timestamp := strings.Replace(gjson.Get(string(byteValue), base+".data.timestamp").Str, "Z", "", 1)
			if timestamp == "" {
				timestamp = time.Time{}.String()
			}
			message := &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(fmt.Sprintf(`{"Key": "%s", "LatestSpeed": %f, "Start": "%s", "Latitude": %f, "Longitude": %f, "Odometer": %f}`, key, speed, timestamp, lat, lon, odometer))}
			p, o, err := producer.SendMessage(message)
			fmt.Println(p, o, key, message.Value)
			if err != nil {
				errors++
				log.Println(err)
			} else {
				successes++
			}
		}
	}

	log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
}
