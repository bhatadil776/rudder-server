package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cancel()

	pulsarURL := os.Getenv("PULSAR_URL")
	topicName := os.Getenv("TOPIC_NAME")
	mode := os.Getenv("MODE")
	if pulsarURL == "" || topicName == "" {
		log.Fatal("Required environment variables are not set: PULSAR_URL or TOPIC_NAME")
	}

	log.Printf(
		"Starting with configuration: Pulsar URL: %s, Topic: %s, Mode: %q\n",
		pulsarURL, topicName, mode,
	)

	start := time.Now()
	defer func() {
		log.Printf("Total time: %s", time.Since(start))
	}()

	var (
		numUsers     = 100
		messageCount = 100
		client       = &http.Client{}
		srvEndpoint  = "http://localhost:8080/"
	)

	switch mode {
	case "ORDER":
		var eg errgroup.Group
		for i := 1; i <= numUsers; i++ {
			userID := i
			eg.Go(func() error {
				return sendMessages(ctx, client, srvEndpoint, userID, messageCount)
			})
		}
		err := eg.Wait()
		if err != nil {
			log.Fatalf("Could not send messages: %v", err)
		}

		log.Println("Done publishing!")
	case "VERIFY":
		// Subscribe and verify messages for all users
		err := verifyMessages(ctx, pulsarURL, topicName, numUsers, messageCount)
		if err != nil {
			log.Printf("Could not verify messages: %v", err)
		}
	case "PERFORMANCE":
		messagePayload := rand.UniqueString(128)

		var eg errgroup.Group
		for i := 1; i <= numUsers; i++ {
			userID := i
			eg.Go(func() error {
				return sendMessagesPerformance(ctx, client, srvEndpoint, userID, messageCount, messagePayload)
			})
		}
		err := eg.Wait()
		if err != nil {
			log.Fatalf("Could not send messages: %v", err)
		}

		log.Println("Done publishing!")
	}
}

func sendMessagesPerformance(
	ctx context.Context, c *http.Client, endpoint string,
	userID, messageCount int, messagePayload string,
) error {
	for i := 1; i <= messageCount; i++ {
		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(messagePayload))
		if err != nil {
			return fmt.Errorf("failed to create request: %v", err)
		}

		req.Header.Set("X-User-ID", fmt.Sprintf("user-%02d", userID))
		req.Header.Set("Content-Type", "text/plain")

		resp, err := c.Do(req)
		if err != nil {
			return fmt.Errorf("could not do request: %v", err)
		}
		err = resp.Body.Close()
		if err != nil {
			return fmt.Errorf("could not close response body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("received non-OK status code: %v", resp.StatusCode)
		}
		fmt.Printf(".")
	}

	log.Printf("User %02d: Sent %d messages", userID, messageCount)
	return nil
}

func sendMessages(ctx context.Context, c *http.Client, endpoint string, userID, messageCount int) error {
	for i := 1; i <= messageCount; i++ {
		message := createMessagePayload(userID, i)

		log.Printf("Sending message: %s", message)

		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(message))
		if err != nil {
			return fmt.Errorf("failed to create request: %v", err)
		}

		req.Header.Set("X-User-ID", fmt.Sprintf("user-%02d", userID))
		req.Header.Set("Content-Type", "text/plain")

		resp, err := c.Do(req)
		if err != nil {
			return fmt.Errorf("could not do request: %v", err)
		}
		err = resp.Body.Close()
		if err != nil {
			return fmt.Errorf("could not close response body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("received non-OK status code: %v", resp.StatusCode)
		}
	}

	log.Printf("User %02d: Sent %d messages", userID, messageCount)
	return nil
}

func verifyMessages(ctx context.Context, pulsarURL, topicName string, numUsers, messageCount int) error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarURL,
	})
	if err != nil {
		return fmt.Errorf("pulsar client error: %v", err)
	}
	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "test-client-subscription",
		Type:             pulsar.KeyShared,
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic: %v", err)
	}
	defer consumer.Close()

	consumedMessages := make(map[string][]string)
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			return fmt.Errorf("consumer receive error: %v", err)
		}

		log.Printf("Got message %q: %s", msg.Key(), msg.Payload())

		consumedMessages[msg.Key()] = append(consumedMessages[msg.Key()], string(msg.Payload()))

		if err := consumer.Ack(msg); err != nil {
			return fmt.Errorf("could not ack message: %v: %v", msg.ID(), err)
		}

		if len(consumedMessages) != numUsers {
			continue
		}
		allDone := true
		for _, messages := range consumedMessages {
			if len(messages) != messageCount {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
	}

	// Verify order of messages
	for userID := 1; userID <= numUsers; userID++ {
		userKey := fmt.Sprintf("user-%02d", userID)
		messages := consumedMessages[userKey]
		if len(messages) != messageCount {
			return fmt.Errorf("incomplete messages received for user %s", userKey)
		}

		for i := 1; i <= messageCount; i++ {
			expectedMessage := createMessagePayload(userID, i)
			receivedMessage := messages[i-1]

			if expectedMessage != receivedMessage {
				return fmt.Errorf(
					"message order verification failed for user %s: expected %s, received %s",
					userKey, expectedMessage, receivedMessage,
				)
			}
		}
	}

	log.Println("Message verification successful")

	return nil
}

func createMessagePayload(userID, messageNumber int) string {
	return fmt.Sprintf("user-%02d: message-%04d", userID, messageNumber)
}
