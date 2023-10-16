package main

import (
	"log"
	"net/http"
	"time"

	"github.com/3rs4lg4d0/go-kafka-checker"
	"github.com/InVisionApp/go-health/v2"
	"github.com/InVisionApp/go-health/v2/handlers"
)

func main() {
	// Create a new health instance.
	h := health.New()

	// Create a kafka check skipping the first three consumer timeouts if any.
	kafkaCheck, err := kafka.NewKafka(kafka.KafkaConfig{
		BootstrapServers:     "localhost:19092",
		SkipConsumerTimeouts: 3,
	})

	if err != nil {
		panic(err)
	}

	// Add the checks to the health instance.
	h.AddChecks([]*health.Config{
		{
			Name:     "kafka-check",
			Checker:  kafkaCheck,
			Interval: 5 * time.Second,
			Fatal:    true,
		},
	})

	//  Start the healthcheck process.
	if err := h.Start(); err != nil {
		log.Fatalf("Unable to start healthcheck: %v", err)
	}

	log.Println("Server listening on :8080")

	// Define a healthcheck endpoint and use the built-in JSON handler.
	http.HandleFunc("/healthcheck", handlers.NewJSONHandlerFunc(h, nil))
	http.ListenAndServe(":8080", nil)
}
