package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/InVisionApp/go-health/v2"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

const (
	consumerGroupPrefix         string        = "health-check-"
	defaultTopic                string        = "health-checks"
	defaultPollTimeout          time.Duration = time.Millisecond * 200
	defaultCheckTimeout         time.Duration = time.Second
	defaultSkipConsumerTimeouts int           = 0
)

// checkTimeoutError is a custom error type used to represent a timeout that
// occurs while executing the status check (while a message is being sent or consumed).
type checkTimeoutError struct {
	message string
}

func (e *checkTimeoutError) Error() string {
	return e.message
}

// KafkaConfig is used for configuring the go-kafka check.
type KafkaConfig struct {
	BootstrapServers     string        // coma separated list of kafka brokers
	Topic                string        // topic to connect to (make sure it exists)
	PollTimeout          time.Duration // time spent fetching the data from the topic
	CheckTimeout         time.Duration // maximum time to wait for the check to complete
	SkipConsumerTimeouts int           // maximum number of check timeouts to skip at the beginning when consuming messages
}

// Kafka implements the "ICheckable" interface.
type Kafka struct {
	config   *KafkaConfig
	producer *kafka.Producer
	consumer *kafka.Consumer
}

// Interface compliance verification.
var _ health.ICheckable = (*Kafka)(nil)

// NewKafka builds a go-kafka check initialized with the provided configuration.
func NewKafka(cfg KafkaConfig) (*Kafka, error) {
	if err := validateKafkaConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid kafka config: %w", err)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
		"group.id":          buildUniqueConsumerGroupId(),
		"auto.offset.reset": "latest",
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	err = c.SubscribeTopics([]string{cfg.Topic}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic: %w", err)
	}

	k := &Kafka{
		config:   &cfg,
		consumer: c,
		producer: p,
	}

	return k, nil
}

// validateKafkaConfig validates the provided configuration ans sets defaults
// for unset properties.
func validateKafkaConfig(cfg *KafkaConfig) error {
	if cfg.BootstrapServers == "" {
		return errors.New("BootstrapServers property is mandatory")
	}

	if cfg.Topic == "" {
		cfg.Topic = defaultTopic
	}

	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = defaultPollTimeout
	}

	if cfg.CheckTimeout <= 0 {
		cfg.CheckTimeout = defaultCheckTimeout
	}

	if cfg.SkipConsumerTimeouts <= 0 {
		cfg.SkipConsumerTimeouts = defaultSkipConsumerTimeouts
	}

	return nil
}

// buildUniqueConsumerGroupId builds a unique consumer group identifier to consume
// health messages independently from other intances (of the same application or other
// applications). This will create a lot of different consumer groups consuming from the
// same health topic that will be unused on every restart of our application so better to
// have a periodic process to cleanup consumer groups in the kafka cluster.
func buildUniqueConsumerGroupId() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknownhost"
	}

	timestamp := time.Now().UnixNano()
	uniqueID := fmt.Sprintf("%s-%s-%d", consumerGroupPrefix, hostname, timestamp)

	return uniqueID
}

// Status function is responsible for executing the Kafka health check. This
// process involves sending a random message to the configured health check topic
// and subsequently confirming the reception of this message by the consumer within
// the predefined timeout constraints.
func (k *Kafka) Status() (interface{}, error) {
	checkStart := time.Now()
	details := make(map[string]string)
	ctx, cancel := context.WithTimeout(context.Background(), k.config.CheckTimeout)
	defer cancel()

	expectedMessage, err := k.sendMessage(ctx)
	if err != nil {
		// If the application starts with a producer error we don't want to skip
		// next consumer iterations so we set the value to zero.
		k.config.SkipConsumerTimeouts = 0
		details["producer"] = err.Error()
		return details, errors.New("error sending messages")
	}

	err = k.consumeMessage(ctx, expectedMessage)
	if err != nil {
		_, isCheckTimeoutError := err.(*checkTimeoutError)
		if isCheckTimeoutError && k.config.SkipConsumerTimeouts > 0 {
			details["info"] = fmt.Sprintf("skipped check timeout (%d remaining)", k.config.SkipConsumerTimeouts-1)
			k.config.SkipConsumerTimeouts--
			return details, nil
		} else {
			details["consumer"] = err.Error()
			return details, errors.New("error receiving messages")
		}
	}

	// Set the property to zero as soon as we complete a roundtrip successfully.
	k.config.SkipConsumerTimeouts = 0
	details["info"] = fmt.Sprintf("Check completed in %v", time.Since(checkStart))
	return details, nil

}

// sendMessage sends a random message to the configured health check topic and waits
// for the delivery report. It returns the generated message and a possible error.
func (k *Kafka) sendMessage(ctx context.Context) (msg string, deliveryErr error) {
	msg = uuid.New().String()
	err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &k.config.Topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
	}, nil)

	if err != nil {
		return "", err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				deliveryErr = &checkTimeoutError{message: "check timeout while waiting for the message delivery report"}
				wg.Done()
				return
			case e := <-k.producer.Events():
				if m, ok := e.(*kafka.Message); ok {
					deliveryErr = m.TopicPartition.Error
					wg.Done()
					return
				}
			}
		}
	}()

	wg.Wait()

	return
}

// consumeMessage starts a consuming loop to check for the expected message. It returns
// nil to indicate whether the expected message was received or an error in other case.
func (k *Kafka) consumeMessage(ctx context.Context, expectedMessage string) error {
	for {
		select {
		// If the context deadline was already reached when producing this case
		// will execute anyway at first place.
		case <-ctx.Done():
			return &checkTimeoutError{message: "check timeout while consuming messages"}
		default:
			ev := k.consumer.Poll(int(k.config.PollTimeout / time.Millisecond))
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				if expectedMessage == string(e.Value) {
					return nil
				}
			case kafka.Error:
				return e
			}
		}
	}
}
