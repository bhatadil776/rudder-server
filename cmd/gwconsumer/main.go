package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	"golang.org/x/sync/errgroup"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cancel()

	// Get the number of nodes from the environment variable
	noOfNodes, ok := readIntEnvVar("NO_OF_NODES")
	if !ok {
		log.Fatalf("required environment variable is not set or is invalid: NO_OF_NODES")
	}

	pulsarURL := os.Getenv("PULSAR_URL")
	topicName := os.Getenv("TOPIC_NAME")
	maxConnectionsPerBroker := os.Getenv("MAX_CONNECTIONS_PER_BROKER")

	log.Printf(
		"Starting with configuration: Pulsar URL: %s, Topic: %s, MaxConnsPerBroker: %s, Nodes: %d\n",
		pulsarURL, topicName, maxConnectionsPerBroker, noOfNodes,
	)

	maxConnectionsPerBrokerInt, err := strconv.Atoi(maxConnectionsPerBroker)
	if err != nil {
		log.Fatal("Invalid MAX_CONNECTIONS_PER_BROKER value:", err)
	}

	err = run(
		ctx,
		pulsarURL, topicName,
		maxConnectionsPerBrokerInt,
		noOfNodes,
	)
	if err != nil {
		log.Fatal("Server error:", err)
	}
}

func run(
	ctx context.Context, pulsarURL, topicName string,
	maxConnectionsPerBroker, noOfNodes int,
) error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     pulsarURL,
		MaxConnectionsPerBroker: maxConnectionsPerBroker,
	})
	if err != nil {
		return fmt.Errorf("pulsar client error: %v", err)
	}
	defer client.Close()

	group, groupCtx := errgroup.WithContext(ctx)

	for i := 0; i < noOfNodes; i++ {
		i := i
		group.Go(func() error {
			instanceTopic := topicName + "-" + strconv.Itoa(i)
			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            instanceTopic,
				SubscriptionName: "gwconsumer",
				Type:             pulsar.KeyShared,
			})
			if err != nil {
				return fmt.Errorf("failed to subscribe to topic %s: %v", instanceTopic, err)
			}
			defer consumer.Close()

			for {
				msg, err := consumer.Receive(groupCtx)
				if err != nil {
					return fmt.Errorf("consumer receive error: %v", err)
				}

				log.Printf("Got message from writeKey %q: %s", msg.Key(), msg.Payload())
			}
		})
	}

	return group.Wait()
}

func readIntEnvVar(name string) (int, bool) {
	rawValue, exists := os.LookupEnv(name)
	if !exists {
		return 0, false
	}

	value, err := strconv.Atoi(rawValue)
	if err != nil {
		return 0, false
	}

	return value, true
}
