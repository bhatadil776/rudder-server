package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cenkalti/backoff/v4"
)

func main() {
	pulsarURL := os.Getenv("PULSAR_URL")
	topicName := os.Getenv("TOPIC_NAME")
	batchMaxDelay := os.Getenv("BATCH_MAX_DELAY")
	batchMaxMessages := os.Getenv("BATCH_MAX_MESSAGES")
	maxConnectionsPerBroker := os.Getenv("MAX_CONNECTIONS_PER_BROKER")
	if pulsarURL == "" || topicName == "" {
		log.Fatal("Required environment variables are not set: PULSAR_URL or TOPIC_NAME")
	}
	batchMaxDelayDuration, err := time.ParseDuration(batchMaxDelay)
	if err != nil {
		log.Fatal("Invalid BATCH_MAX_DELAY value:", err)
	}
	batchMaxMessagesInt, err := strconv.Atoi(batchMaxMessages)
	if err != nil {
		log.Fatal("Invalid BATCH_MAX_MESSAGES value:", err)
	}
	maxConnectionsPerBrokerInt, err := strconv.Atoi(maxConnectionsPerBroker)
	if err != nil {
		log.Fatal("Invalid MAX_CONNECTIONS_PER_BROKER value:", err)
	}

	log.Printf("Starting with configuration: Pulsar URL: %s, Topic: %s, BatchMaxDelay: %s, BatchMaxMessages: %d\n",
		pulsarURL, topicName, batchMaxDelayDuration, batchMaxMessagesInt,
	)

	err = run(pulsarURL, topicName, batchMaxDelayDuration, batchMaxMessagesInt, maxConnectionsPerBrokerInt)
	if err != nil {
		log.Fatal("Server error:", err)
	}
}

func run(
	pulsarURL,
	topicName string,
	batchMaxDelay time.Duration,
	batchMaxMessages int,
	maxConnectionsPerBroker int,
) error {
	// Create Pulsar client and producer
	log.Printf("Starting client with URL %q and MaxConnectionsPerBroker %d",
		pulsarURL, maxConnectionsPerBroker,
	)
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     pulsarURL,
		MaxConnectionsPerBroker: maxConnectionsPerBroker,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	producerOpts := pulsar.ProducerOptions{
		Topic:                   topicName,
		BatchingMaxPublishDelay: batchMaxDelay,
		BatchingMaxMessages:     uint(batchMaxMessages),
		MaxPendingMessages:      batchMaxMessages,
		BatchingMaxSize:         5242880, // 5 MB
		BatcherBuilderType:      pulsar.KeyBasedBatchBuilder,
		// Murmur3_32Hash: MurmurHash3 operates on 4 bytes at a time,
		// and involves a series of multiply, add, and bitwise shift
		// operations to mix the bits thoroughly. This results in a
		// more even distribution of hash values and is more performant
		// than JavaStringHash, especially for longer keys.
		HashingScheme: pulsar.Murmur3_32Hash,
		// Using custom message router to simplify testing the message throughput
		MessageRouter: func(msg *pulsar.ProducerMessage, metadata pulsar.TopicMetadata) int {
			userID := msg.Key
			numPartitions := metadata.NumPartitions()
			partition, err := strconv.Atoi(userID)
			if err != nil {
				return -1 // default to round-robin
			}
			if uint32(partition) >= numPartitions {
				return -1 // default to round-robin
			}
			return partition
		},
		SendTimeout:   3 * time.Minute,
		BackoffPolicy: backoffAdapter{backoff: backoff.NewConstantBackOff(time.Second)},
	}

	log.Printf("Starting producer with configuration %+v", producerOpts)

	producer, err := client.CreateProducer(producerOpts)
	if err != nil {
		return err
	}
	defer producer.Close()

	// Create HTTP server
	server := &http.Server{
		Addr: ":8080",
	}

	// Mutex and variable to store the maximum latency
	var (
		maxLatency      int64
		maxLatencyMutex sync.Mutex
	)

	// Define the HTTP handler
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Println("Failed to read request body:", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		userID := r.Header.Get("X-User-ID")
		if userID == "" {
			log.Println("Missing X-User-ID header")
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		ctx := r.Context()

		// Publish request body to Pulsar topic with the specified timeout
		msg := pulsar.ProducerMessage{
			Key:     userID,
			Payload: body,
		}

		done := make(chan error, 1)
		producer.SendAsync(ctx, &msg, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
			done <- err
		})

		err = <-done
		if err != nil {
			log.Println("Failed to publish message to Pulsar:", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		// Calculate the latency
		latency := time.Since(startTime).Milliseconds()

		// Update the maximum latency using a mutex for concurrency safety
		maxLatencyMutex.Lock()
		defer maxLatencyMutex.Unlock()
		if latency > maxLatency {
			maxLatency = latency
			log.Printf("New max latency: %d ms", latency)
		}
	})

	// Start the server in a separate goroutine
	go func() {
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Create a context that gets canceled when a termination signal is received
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer func() {
		cancel()
		if err := server.Shutdown(context.Background()); err != nil {
			log.Printf("Failed to shutdown server gracefully: %v", err)
		}
	}()

	// Wait for the context to be canceled (termination signal received)
	<-ctx.Done()

	log.Println("Shutting down server...")

	// Create a context with a timeout for the server shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Gracefully shutdown the server with the specified timeout
	err = server.Shutdown(shutdownCtx)
	if err != nil {
		return err
	}

	return nil
}

type backoffAdapter struct {
	backoff backoff.BackOff
}

func (b backoffAdapter) Next() time.Duration {
	next := b.backoff.NextBackOff()
	log.Println("Next backoff:", next)
	return next
}
