package config

type Settings struct {
	MonPort string `yaml:"MON_PORT"`

	KafkaBrokers      string `yaml:"KAFKA_BROKERS"`
	ConsumerGroup     string `yaml:"CONSUMER_GROUP"`
	DeviceStatusTopic string `yaml:"DEVICE_STATUS_TOPIC"`
	TripEventTopic    string `yaml:"TRIP_EVENT_TOPIC"`
}
