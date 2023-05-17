package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	pulsarURL := os.Getenv("PULSAR_URL")
	topicName := os.Getenv("TOPIC_NAME")

	log.Printf("Starting with configuration: Pulsar URL: %s, Topic: %s\n", pulsarURL, topicName)

	err := run(pulsarURL, topicName)
	if err != nil {
		log.Fatal("Server error:", err)
	}
}

func run(pulsarURL, topicName string) error {
	// Create Pulsar client and producer
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarURL,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   topicName,
		MaxPendingMessages:      100,
		BatchingMaxPublishDelay: time.Second,
		BatchingMaxMessages:     100,
		BatchingMaxSize:         5242880, // 5 MB
		BatcherBuilderType:      pulsar.KeyBasedBatchBuilder,
	})
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

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

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
