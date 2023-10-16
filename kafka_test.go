package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

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
	t.Run("Run until the system is stable", func(t *testing.T) {
		ctx := context.Background()
		bootStrapServers, _ := kafkaContainer.Brokers(ctx)
		kafkaCheck, _ := NewKafka(KafkaConfig{
			BootstrapServers: bootStrapServers[0],
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

	t.Run("Check that the first 2 steps are skipped", func(t *testing.T) {
		ctx := context.Background()
		skip := 2
		bootStrapServers, _ := kafkaContainer.Brokers(ctx)
		kafkaCheck, _ := NewKafka(KafkaConfig{
			BootstrapServers:     bootStrapServers[0],
			SkipConsumerTimeouts: skip,
		})
		defer kafkaCheck.consumer.Close()
		defer kafkaCheck.producer.Close()

		done := time.After(120 * time.Second)
		ticker := time.NewTicker(5 * time.Second)
		for skip > 0 {
			select {
			case <-done:
				t.Fatal("The test couldn't complete in two minutes")
			case <-ticker.C:
				details, err := kafkaCheck.Status()
				assert.NoError(t, err)
				fmt.Printf("[OK] details=%v\n", details)
				skip--
				assert.Equal(t, map[string]string{"info": fmt.Sprintf("skipped check timeout (%d remaining)", skip)}, details)
			}
		}
	})
}
