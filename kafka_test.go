package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

var (
	kafkaContainer *kafka.KafkaContainer
)

func TestMain(m *testing.M) {
	var err error
	ctx := context.Background()
	kafkaContainer, err = kafka.RunContainer(ctx,
		kafka.WithClusterID("test-cluster"),
		testcontainers.WithImage("confluentinc/confluent-local:7.5.1"),
	)
	if err != nil {
		os.Exit(1)
	}

	_, err = kafkaContainer.State(ctx)
	if err != nil {
		os.Exit(1)
	}

	code := m.Run()

	if err = kafkaContainer.Terminate(ctx); err != nil {
		os.Exit(1)
	}

	os.Exit(code)
}

func TestBuildUniqueConsumerGroupId(t *testing.T) {
	realHostname, _ := os.Hostname()

	type args struct {
		hnProvider hostnameProvider
	}
	testcases := []struct {
		name         string
		args         args
		wantHostname string
	}{
		{
			name: "return the hostname",
			args: args{
				hnProvider: os.Hostname,
			},
			wantHostname: realHostname,
		},
		{
			name: "return unknownhost is an error happens",
			args: args{
				hnProvider: func() (string, error) {
					return "", errors.New("unexpected error")
				},
			},
			wantHostname: "unknownhost",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			hnProvider = tc.args.hnProvider
			cg := buildUniqueConsumerGroupId()
			substrings := strings.Split(cg, "@@")
			assert.Equal(t, tc.wantHostname, substrings[1])
		})
	}
}

func TestNewKafka(t *testing.T) {
	ctx := context.Background()
	bootStrapServers, _ := kafkaContainer.Brokers(ctx)

	type args struct {
		cfg KafkaConfig
	}
	testcases := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "return error because BootstrapServers is mandatory",
			args: args{
				cfg: KafkaConfig{},
			},
			wantErr: true,
		},
		{
			name: "return error because BootstrapServers is mandatory",
			args: args{
				cfg: KafkaConfig{
					BootstrapServers: bootStrapServers[0],
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			kafkaCheck, err := NewKafka(tc.args.cfg)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				kafkaCheck.consumer.Close()
				kafkaCheck.producer.Close()
			}
		})
	}
}

func TestStatus(t *testing.T) {
	t.Run("run until the system is stable", func(t *testing.T) {
		ctx := context.Background()
		bootStrapServers, _ := kafkaContainer.Brokers(ctx)
		kafkaCheck, _ := NewKafka(KafkaConfig{
			BootstrapServers: bootStrapServers[0],
			CheckTimeout:     400 * time.Millisecond,
		})
		defer kafkaCheck.consumer.Close()
		defer kafkaCheck.producer.Close()

		done := time.After(120 * time.Second)
		ticker := time.NewTicker(5 * time.Second)
		for successStatuses := 0; successStatuses < 2; {
			select {
			case <-done:
				t.Fatal("The test couldn't complete in two minutes")
			case <-ticker.C:
				details, err := kafkaCheck.Status()
				if err != nil {
					fmt.Printf("[KO, still booting the infra] err=%v, details=%v\n", err, details)
				} else {
					fmt.Printf("[OK] details=%v\n", details)
					successStatuses++
				}
			}
		}
	})

	t.Run("check that at least 1 check timeout is skipped", func(t *testing.T) {
		ctx := context.Background()
		iterations := 2
		skipped := 0

		bootStrapServers, _ := kafkaContainer.Brokers(ctx)
		kafkaCheck, _ := NewKafka(KafkaConfig{
			BootstrapServers:     bootStrapServers[0],
			CheckTimeout:         400 * time.Millisecond,
			SkipConsumerTimeouts: iterations,
		})
		defer kafkaCheck.consumer.Close()
		defer kafkaCheck.producer.Close()

		done := time.After(120 * time.Second)
		ticker := time.NewTicker(5 * time.Second)
		for iterations > 0 {
			select {
			case <-done:
				t.Fatal("The test couldn't complete in two minutes")
			case <-ticker.C:
				details, err := kafkaCheck.Status()
				assert.NoError(t, err)
				fmt.Printf("[OK] details=%v\n", details)
				iterations--
				if reflect.DeepEqual(map[string]string{"info": fmt.Sprintf("skipped check timeout (%d remaining)", iterations)}, details) {
					skipped++
				}
			}
		}
		assert.True(t, skipped > 0)
	})

	t.Run("set an invalid topic to force a producer error", func(t *testing.T) {
		ctx := context.Background()
		bootStrapServers, _ := kafkaContainer.Brokers(ctx)
		c, _ := confluentKafka.NewConsumer(&confluentKafka.ConfigMap{
			"bootstrap.servers": bootStrapServers,
			"group.id":          buildUniqueConsumerGroupId(),
			"auto.offset.reset": "latest",
		})
		p, _ := confluentKafka.NewProducer(&confluentKafka.ConfigMap{
			"bootstrap.servers": bootStrapServers,
		})
		kafkaCheck := &Kafka{
			config: &KafkaConfig{
				BootstrapServers: bootStrapServers[0],
				Topic:            "",
			},
			consumer: c,
			producer: p,
		}

		_, err := kafkaCheck.Status()
		assert.Equal(t, "error sending messages", err.Error())
	})
}
