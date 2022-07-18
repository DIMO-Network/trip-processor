package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/tidwall/gjson"
)

const topic string = "test-topic"
const partitions int32 = 9

var brokerURLs = []string{"localhost:9093"}

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// hash keys to make sure all messages with the same key end up in the same partition
	config.Producer.Partitioner = sarama.NewHashPartitioner

	admin, err := sarama.NewClusterAdmin(brokerURLs, config)
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

	client, err := sarama.NewClient(brokerURLs, config)
	defer client.Close()
	if err != nil {
		log.Fatal(err)
	}
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatal(err)
	}

	var successes, errors int

	jsonFile, err := os.Open("data.json")
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)

	numResponses := gjson.Get(string(byteValue), "rawResponse.hits.hits.#").Int()

	for i := 0; int64(i) < numResponses; i++ {
		base := fmt.Sprintf("rawResponse.hits.hits.%d.fields", i)
		speed := gjson.Get(string(byteValue), base+".data\\.payload\\.data\\.speed.0").Int()
		timestamp := gjson.Get(string(byteValue), base+".data\\.payload\\.time.0").Time()

		message := &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(fmt.Sprintf("VehicleID: %s", "uniqueTestID")), Value: sarama.StringEncoder(fmt.Sprintf(`{"Speed": %d, "Timestamp": %t}`, speed, timestamp))}
		// also returns partition and offset.. do we need this for anything?
		_, _, err := producer.SendMessage(message)
		if err != nil {
			errors++
			log.Println(err)
		} else {
			successes++
		}
		time.Sleep(2 * time.Second)
	}

	log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
}
