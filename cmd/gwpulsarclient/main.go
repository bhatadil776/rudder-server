package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cenkalti/backoff"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cancel()

	pulsarURL := os.Getenv("PULSAR_URL")
	topicName := os.Getenv("TOPIC_NAME")
	srvEndpoint := os.Getenv("SERVER_ENDPOINT")
	numSources := os.Getenv("NUM_SOURCES")
	usersPerSource := os.Getenv("USERS_PER_SOURCE")
	messagesPerUser := os.Getenv("MESSAGES_PER_USER")
	mode := os.Getenv("MODE")

	log.Printf(
		"Starting with configuration: Pulsar URL: %s, Topic: %s, Mode: %q, Srv: %s, Users: %s, Msgs: %s\n",
		pulsarURL, topicName, mode, srvEndpoint, numSources, messagesPerUser,
	)

	numSourcesInt, err := strconv.Atoi(numSources)
	if err != nil {
		log.Fatal("Invalid NUM_SOURCES value:", err)
	}
	usersPerSourceInt, err := strconv.Atoi(usersPerSource)
	if err != nil {
		log.Fatal("Invalid USERS_PER_SOURCE value:", err)
	}
	messagesPerUserInt, err := strconv.Atoi(messagesPerUser)
	if err != nil {
		log.Fatal("Invalid MESSAGES_PER_USER value:", err)
	}

	client := &http.Client{}

	switch mode {
	case "ORDER":
		start := time.Now()
		var eg errgroup.Group
		for i := 0; i < numSourcesInt; i++ {
			writeKey := i
			for j := 0; j < usersPerSourceInt; j++ {
				userID := j
				eg.Go(func() error {
					return sendMessages(ctx, client, srvEndpoint, writeKey, userID, messagesPerUserInt)
				})
			}
		}
		err := eg.Wait()
		if err != nil {
			log.Fatalf("Could not send messages: %v", err)
		}

		log.Printf("Total time: %s", time.Since(start))
		log.Printf("Message throughput: %f msg/s",
			float64(numSourcesInt*usersPerSourceInt*messagesPerUserInt)/time.Since(start).Seconds(),
		)
		log.Println("Done publishing!")
	case "VERIFY":
		if pulsarURL == "" || topicName == "" {
			log.Fatal("Required environment variables are not set: PULSAR_URL or TOPIC_NAME")
		}

		// Subscribe and verify messages for all users
		err := verifyMessages(ctx, pulsarURL, topicName, numSourcesInt, usersPerSourceInt, messagesPerUserInt)
		if err != nil {
			log.Printf("Could not verify messages: %v", err)
		}
	}
}

func sendMessages(
	ctx context.Context, c *http.Client, endpoint string,
	writeKey, userID, messageCount int,
) error {
	var sent int
	for i := 1; i <= messageCount; i++ {
		message := createMessagePayload(userID, i)

		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(message))
		if err != nil {
			return fmt.Errorf("failed to create request: %v", err)
		}

		req.Header.Set("X-WriteKey", strconv.Itoa(writeKey))
		req.Header.Set("Content-Type", "text/plain")

		var resp *http.Response
		operation := func() error {
			resp, err = c.Do(req)
			return err
		}
		backoffWithMaxRetry := backoff.WithContext(
			backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3), ctx,
		)
		err = backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
			log.Printf("Failed to POST with error: %v, retrying after %v", err, t)
		})
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

		sent++
		if sent%100 == 0 {
			log.Printf("WriteKey: %d, User %d: Sent %d messages", writeKey, userID, sent)
		}
	}

	log.Printf("User %02d: Sent %d messages", userID, messageCount)
	return nil
}

func verifyMessages(
	ctx context.Context, pulsarURL, topicName string,
	numSources, usersPerSource, messagesPerUser int,
) error {
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

	consumedMessages := make(map[string]map[string][]string)
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			return fmt.Errorf("consumer receive error: %v", err)
		}

		log.Printf("Got message from writeKey %q: %s", msg.Key(), msg.Payload())

		if consumedMessages[msg.Key()] == nil {
			consumedMessages[msg.Key()] = make(map[string][]string)
		}

		data := strings.Split(string(msg.Payload()), ":")
		consumedMessages[msg.Key()][data[0]] = append(consumedMessages[msg.Key()][data[0]], data[1])

		if err := consumer.Ack(msg); err != nil {
			return fmt.Errorf("could not ack message: %v: %v", msg.ID(), err)
		}

		if len(consumedMessages) != numSources {
			continue
		}
		allDone := true
		for _, users := range consumedMessages {
			if len(users) != usersPerSource {
				allDone = false
				break
			}
			for _, messages := range users {
				if len(messages) != messagesPerUser {
					allDone = false
					break
				}
			}
		}
		if allDone {
			break
		}
	}

	// Verify order of messages
	for writeKey := 0; writeKey < numSources; writeKey++ {
		key := fmt.Sprintf("%d", writeKey)
		for userID, messages := range consumedMessages[key] {
			for i := 1; i <= messagesPerUser; i++ {
				expectedMessage := fmt.Sprintf("%d", i)
				receivedMessage := messages[i-1]

				if expectedMessage != receivedMessage {
					return fmt.Errorf(
						"message order verification failed for writeKey %d user %s: expected %s, received %s",
						writeKey, userID, expectedMessage, receivedMessage,
					)
				}
			}
		}
	}

	log.Println("Message verification successful")

	return nil
}

func createMessagePayload(userID, messageNumber int) string {
	return strconv.Itoa(userID) + ":" + strconv.Itoa(messageNumber)
}
