[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

# go-kafka-checker

An Apache Kafka checker for *InVisionApp* [go-health](https://github.com/InVisionApp/go-health).

## How it works

The Apache Kafka checker implements a simple roundtrip check that sends a random message to a specific topic and checks that the message is received within the time specified in the [configuration](#configuration).

## Installation

```bash
go get github.com/3rs4lg4d0/go-kafka-checker
```

## Usage

```go
// Create a new health instance.
h := health.New()

// Create a kafka check skipping the first three consumer timeouts (as maximum) if any.
kafkaCheck, err := kafka.NewKafka(kafka.KafkaConfig{
	BootstrapServers:     "localhost:19092",
	SkipConsumerTimeouts: 3,
})
```

You have a complete example [here](internal/example.go).

## Configuration

You can configure the checker using the `KafkaConfig` struct.

```go
type KafkaConfig struct {
	BootstrapServers       string
	Topic                  string
	PollTimeout            time.Duration
	CheckTimeout           time.Duration
	SkipConsumerIterations int
}
```

| Property | Type | Default value | Description |
| --- | --- | --- | ---- |
| BootstrapServers | string | - | Coma separated list of kafka brokers. This property is **mandatory**. |
| Topic | string | health-checks | Topic to connect to (make sure it exists). |
| PollTimeout | time.Duration | 200 ms | The maximum time spent fetching data from the topic. |
| CheckTimeout | time.Duration | 1000 ms | The maximum time to wait for the check to complete. |
| SkipConsumerTimeouts | int | 0 | Maximum number of check timeouts to skip at the beginning when consuming messages. |

`SkipConsumerTimeouts` can be a useful property to avoid some unhealthy results because joining to a consumer group and receiving a partition assignment can take some time. So if you provide a value for this property greater than zero, for the first `n` checks (as maximum), if any timeout happens when consuming messages, you will see an output like this:

```json
{
  "details": {
    "kafka-check": {
      "name": "kafka-check",
      "status": "ok",
      "fatal": true,
      "details": {
        "info": "skipped check timeout (2 remaining)"
      },
      "check_time": "2023-10-17T00:48:57.51717+02:00",
      "num_failures": 0,
      "first_failure_at": "0001-01-01T00:00:00Z"
    }
  },
  "status": "ok"
}
```

If another type of error happens during the first checks (e.g. a Kafka error), even if you set this property to a value greater than zero, `SkipConsumerTimeouts` is internally set to zero. This also happens as soon as a check completes successfully.