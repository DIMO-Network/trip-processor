package consumer

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/DIMO-Network/shared"
	"github.com/DIMO-Network/shared/kafka"
	"github.com/DIMO-Network/trips-api/internal/config"
	"github.com/DIMO-Network/trips-api/services/segmenter"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const elasticSearchMaxSize = 10000

type CompletedSegmentConsumer struct {
	config kafka.Config
	logger *zerolog.Logger
	Client *elasticsearch.Client
	Index  string
}

func New(settings *config.Settings, logger *zerolog.Logger) (*CompletedSegmentConsumer, error) {
	kc := kafka.Config{
		Brokers: strings.Split(settings.KafkaBrokers, ","),
		Topic:   settings.TripEventTopic,
		Group:   "segmenter",
	}

	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{settings.ElasticHost},
		Username:  settings.ElasticUsername,
		Password:  settings.ElasticPassword,
	})
	if err != nil {
		return nil, err
	}

	return &CompletedSegmentConsumer{
		config: kc,
		logger: logger,
		Client: es,
		Index:  settings.ElasticIndex,
	}, nil
}

func (c *CompletedSegmentConsumer) Start(ctx context.Context) {
	if err := kafka.Consume(ctx, c.config, c.ingest, c.logger); err != nil {
		c.logger.Fatal().Err(err).Msg("Couldn't start segment consumer.")
	}

	c.logger.Info().Msg("segment consumer started.")
}

func (c *CompletedSegmentConsumer) ingest(ctx context.Context, event *shared.CloudEvent[segmenter.SegmentEvent]) error {
	response, err := c.fetchData(event.Data.DeviceID, event.Data.Start.Format(time.RFC3339), event.Data.End.Format(time.RFC3339))
	if err != nil {
		return err
	}

	compressedData, err := c.compress(response, event.Data.DeviceID, event.Data.Start.Format(time.RFC3339), event.Data.End.Format(time.RFC3339))
	if err != nil {
		return err
	}

	encryptedData, err := c.encrypt(compressedData)
	if err != nil {
		return err
	}

	err = c.upload(encryptedData)
	if err != nil {
		return err
	}

	return nil
}

func (c *CompletedSegmentConsumer) fetchData(deviceID, start, end string) ([]byte, error) {
	var searchAfter int64
	query := QueryTrip{
		Sort: []map[string]string{
			{"data.timestamp": "asc"},
		},
		Query: map[string]any{
			"bool": map[string]any{
				"filter": []map[string]any{
					{
						"range": map[string]any{
							"data.timestamp": map[string]interface{}{
								"format": "strict_date_optional_time",
								"gte":    start,
								"lte":    end,
							},
						},
					},
				},
				"must": map[string]any{
					"match": map[string]string{
						"subject": deviceID,
					},
				},
			},
		},
		Size: elasticSearchMaxSize,
	}

	response, err := c.executeESQuery(query)
	if err != nil {
		return []byte{}, err
	}

	n := gjson.GetBytes(response, "hits.hits.#").Int()
	searchAfter = gjson.GetBytes(response, fmt.Sprintf("hits.hits.%d.sort.0", n-1)).Int()

	for searchAfter > 0 {
		query.SearchAfter = []int64{searchAfter}
		resp, err := c.executeESQuery(query)
		if err != nil {
			return []byte{}, err
		}

		n = gjson.GetBytes(resp, "hits.hits.#").Int()
		searchAfter = gjson.GetBytes(resp, fmt.Sprintf("hits.hits.%d.sort.0", n-1)).Int()

		for i := 0; int64(i) < n; i++ {
			response, err = sjson.SetBytes(response, "hits.hits.-1", gjson.GetBytes(resp, fmt.Sprintf("hits.hits.%d", i)).String())
			if err != nil {
				return []byte{}, err
			}
		}
	}

	return response, nil
}

func (c *CompletedSegmentConsumer) compress(data []byte, deviceID, start, end string) ([]byte, error) {
	fmt.Println("Length of Data: ", len(data))
	b := new(bytes.Buffer)
	zw := zip.NewWriter(b)

	nameCompressed := fmt.Sprintf("%s_%s.json", start, end)
	file, err := zw.Create(nameCompressed)
	if err != nil {
		return nil, err
	}

	_, err = file.Write(data)
	if err != nil {
		return nil, err
	}

	err = zw.Close()
	if err != nil {
		return nil, err
	}

	fmt.Println("Length of Bytes: ", len(b.Bytes()))
	// for testing/ qc
	// err = os.WriteFile(fmt.Sprintf("segments-%s.zip", deviceID), b.Bytes(), 0777)
	// if err != nil {
	// 	return nil, err
	// }

	return b.Bytes(), nil
}

func (c *CompletedSegmentConsumer) encrypt(data []byte) ([]byte, error) {
	// TODO
	return []byte{}, nil
}

func (c *CompletedSegmentConsumer) upload(data []byte) error {
	// TODO
	return nil
}

func (c *CompletedSegmentConsumer) executeESQuery(query any) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return []byte{}, err
	}

	res, err := c.Client.Search(
		c.Client.Search.WithContext(context.Background()),
		c.Client.Search.WithIndex(c.Index),
		c.Client.Search.WithBody(&buf),
	)
	if err != nil {
		c.logger.Err(err).Msg("Could not query Elasticsearch")
		return []byte{}, err
	}
	defer res.Body.Close()

	responseBytes, err := io.ReadAll(res.Body)
	if err != nil {
		c.logger.Err(err).Msg("Could not parse Elasticsearch response body")
		return responseBytes, err
	}

	if res.StatusCode >= 400 {
		err := fmt.Errorf("invalid status code when querying elastic: %d", res.StatusCode)
		return responseBytes, err
	}

	return responseBytes, nil
}

type QueryTrip struct {
	Sort        []map[string]string `json:"sort"`
	Source      []string            `json:"_source,omitempty"`
	Size        int                 `json:"size"`
	Query       map[string]any      `json:"query"`
	SearchAfter []int64             `json:"search_after,omitempty"`
}
