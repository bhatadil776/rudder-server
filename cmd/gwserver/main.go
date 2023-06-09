package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cenkalti/backoff/v4"
)

func main() {
	hostname := os.Getenv("HOSTNAME")
	re := regexp.MustCompile(`^gwserver-statefulset-(\d+)$`)
	match := re.FindStringSubmatch(hostname)
	if match == nil {
		log.Fatalf("The hostname %q does not match the pattern: %v", os.Getenv("HOSTNAME"), re)
	}
	instanceNumber, _ := strconv.Atoi(match[1])

	pulsarURL := os.Getenv("PULSAR_URL")
	topicName := os.Getenv("TOPIC_NAME")
	batchMaxDelay := os.Getenv("BATCH_MAX_DELAY")
	batchMaxMessages := os.Getenv("BATCH_MAX_MESSAGES")
	maxConnectionsPerBroker := os.Getenv("MAX_CONNECTIONS_PER_BROKER")
	customMessageRouter := os.Getenv("CUSTOM_MESSAGE_ROUTER")
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
	customMessageRouterBool, err := strconv.ParseBool(customMessageRouter)
	if err != nil {
		log.Fatal("Invalid CUSTOM_MESSAGE_ROUTER value:", err)
	}

	instanceTopic := topicName + "-" + strconv.Itoa(instanceNumber)
	log.Printf(
		"Starting with configuration: Pulsar URL: %s, Topic: %s, BatchMaxDelay: %s, BatchMaxMessages: %d\n",
		pulsarURL, instanceTopic, batchMaxDelayDuration, batchMaxMessagesInt,
	)

	err = run(
		pulsarURL, instanceTopic, batchMaxDelayDuration, batchMaxMessagesInt,
		maxConnectionsPerBrokerInt, customMessageRouterBool,
	)
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
	customMessageRouter bool,
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
		DisableBatching:         false,
		SendTimeout:             3 * time.Minute,
		BackoffPolicy:           backoffAdapter{backoff: backoff.NewConstantBackOff(time.Second)},
	}
	if customMessageRouter {
		// Using custom message router to simplify testing the message throughput
		producerOpts.MessageRouter = func(msg *pulsar.ProducerMessage, metadata pulsar.TopicMetadata) int {
			numPartitions := metadata.NumPartitions()
			partition, err := strconv.Atoi(msg.Key)
			if err != nil {
				return -1 // default to round-robin
			}
			if uint32(partition) >= numPartitions {
				log.Printf("Invalid partition key %q with partitions %d", msg.Key, numPartitions)
				return -1 // default to round-robin
			}
			return partition
		}
	} else {
		// Murmur3_32Hash: MurmurHash3 operates on 4 bytes at a time,
		// and involves a series of multiply, add, and bitwise shift
		// operations to mix the bits thoroughly. This results in a
		// more even distribution of hash values and is more performant
		// than JavaStringHash, especially for longer keys.
		producerOpts.HashingScheme = pulsar.Murmur3_32Hash
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

		writeKey := r.Header.Get("X-WriteKey")
		if writeKey == "" {
			log.Println("Missing X-WriteKey header")
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		ctx := r.Context()

		// Publish request body to Pulsar topic with the specified timeout
		msg := pulsar.ProducerMessage{
			Key:     writeKey,
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
