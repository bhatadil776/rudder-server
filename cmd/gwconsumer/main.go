package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cancel()

	pulsarURL := os.Getenv("PULSAR_URL")
	topicName := os.Getenv("TOPIC_NAME")
	maxConnectionsPerBroker := os.Getenv("MAX_CONNECTIONS_PER_BROKER")

	log.Printf(
		"Starting with configuration: Pulsar URL: %s, Topic: %s, MaxConnsPerBroker: %s\n",
		pulsarURL, topicName, maxConnectionsPerBroker,
	)

	maxConnectionsPerBrokerInt, err := strconv.Atoi(maxConnectionsPerBroker)
	if err != nil {
		log.Fatal("Invalid MAX_CONNECTIONS_PER_BROKER value:", err)
	}

	err = run(
		ctx,
		pulsarURL, topicName,
		maxConnectionsPerBrokerInt,
	)
	if err != nil {
		log.Fatal("Server error:", err)
	}
}

func run(
	ctx context.Context, pulsarURL, topicName string,
	maxConnectionsPerBroker int,
) error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     pulsarURL,
		MaxConnectionsPerBroker: maxConnectionsPerBroker,
	})
	if err != nil {
		return fmt.Errorf("pulsar client error: %v", err)
	}
	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "gwconsumer",
		Type:             pulsar.KeyShared,
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic: %v", err)
	}
	defer consumer.Close()

	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			return fmt.Errorf("consumer receive error: %v", err)
		}

		log.Printf("Got message from writeKey %q: %s", msg.Key(), msg.Payload())
	}
}
