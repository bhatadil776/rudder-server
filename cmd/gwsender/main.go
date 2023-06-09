package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
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

	mode := os.Getenv("MODE")
	pulsarURL := os.Getenv("PULSAR_URL")
	topicName := os.Getenv("TOPIC_NAME")
	srvEndpoint := os.Getenv("SERVER_ENDPOINT")
	noOfNodes := mustReadIntEnvVar("NO_OF_NODES")
	numSources := mustReadIntEnvVar("NUM_SOURCES")
	usersPerSource := mustReadIntEnvVar("USERS_PER_SOURCE")
	messagesPerUser := mustReadIntEnvVar("MESSAGES_PER_USER")

	switch mode {
	case "ORDER":
		log.Printf(
			"Starting with configuration: Mode: %q, SrvEndpoint: %s, "+
				"NoOfNodes: %d, Sources: %d, Users per source: %d, Msgs per user: %d\n",
			mode, srvEndpoint,
			noOfNodes, numSources, usersPerSource, messagesPerUser,
		)

		start := time.Now()
		var eg errgroup.Group
		for k := 0; k < noOfNodes; k++ {
			var (
				nodeNumber = k
				client     = newHTTPClient(10, 10)
			)
			for i := 0; i < numSources; i++ {
				writeKey := i
				for j := 0; j < usersPerSource; j++ {
					userID := j
					eg.Go(func() error {
						return sendMessages(ctx, client, srvEndpoint,
							nodeNumber, writeKey, userID, messagesPerUser,
						)
					})
				}
			}
		}

		err := eg.Wait()
		if err != nil {
			log.Fatalf("Could not send messages: %v", err)
		}

		log.Printf("Total time: %s", time.Since(start))
		log.Printf("Message throughput: %f msg/s",
			float64(noOfNodes*numSources*usersPerSource*messagesPerUser)/time.Since(start).Seconds(),
		)
		log.Println("Done publishing!")
	case "VERIFY":
		if pulsarURL == "" || topicName == "" {
			log.Fatal("Required environment variables are not set: PULSAR_URL or TOPIC_NAME")
		}

		log.Printf(
			"Starting with configuration: Mode: %q, Pulsar URL: %s, Topic: %s\n",
			mode, pulsarURL, topicName,
		)

		// Subscribe and verify messages for all users
		var eg errgroup.Group
		for k := 0; k < noOfNodes; k++ {
			instanceTopic := topicName + "-" + strconv.Itoa(k)
			eg.Go(func() error {
				return verifyMessages(ctx, pulsarURL, instanceTopic,
					numSources, usersPerSource, messagesPerUser,
				)
			})
		}
		if err := eg.Wait(); err != nil {
			log.Printf("Could not verify messages: %v", err)
		}
	}
}

func sendMessages(
	ctx context.Context, c *http.Client, endpoint string,
	nodeNumber, writeKey, userID, messageCount int,
) error {
	var sent int
	for i := 1; i <= messageCount; i++ {
		message := createMessagePayload(userID, i)

		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(message))
		if err != nil {
			log.Printf("[ERROR] Failed to create request: %v", err)
			return fmt.Errorf("failed to create request: %v", err)
		}

		req.Header.Set("X-RoutingKey", strconv.Itoa(nodeNumber))
		req.Header.Set("X-WriteKey", strconv.Itoa(writeKey))
		req.Header.Set("Content-Type", "text/plain")

		var resp *http.Response
		operation := func() error {
			resp, err = c.Do(req)
			defer closeResponse(resp)
			return err
		}
		backoffWithMaxRetry := backoff.WithContext(
			backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 10), ctx,
		)
		err = backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
			// log.Printf("[WARN] Failed to POST with error: %v, retrying after %v", err, t)
		})
		if err != nil {
			log.Printf("[ERROR] Failed to POST with error: %v", err)
			return fmt.Errorf("could not do request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("[ERROR] Received non-OK status code: %v", resp.StatusCode)
			return fmt.Errorf("received non-OK status code: %v", resp.StatusCode)
		}

		sent++
		if sent%100 == 0 {
			log.Printf("Node: %d, WriteKey: %d, User %d: Sent %d messages",
				nodeNumber, writeKey, userID, sent,
			)
		}
	}

	log.Printf("User %02d: Sent %d messages to node %d and writeKey %d",
		userID, messageCount, nodeNumber, writeKey,
	)
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
		SubscriptionName: "gwverifier",
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

func newHTTPClient(maxIdleConns, maxConnsPerHost int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 10 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 10 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
			MaxIdleConns:          maxIdleConns,
			MaxConnsPerHost:       maxConnsPerHost,
		},
		Timeout: 10 * time.Second,
	}
}

func createMessagePayload(userID, messageNumber int) string {
	return strconv.Itoa(userID) + ":" + strconv.Itoa(messageNumber)
}

func closeResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		const maxBodySlurpSize = 2 << 10 // 2KB
		_, _ = io.CopyN(io.Discard, resp.Body, maxBodySlurpSize)
		_ = resp.Body.Close()
	}
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

func mustReadIntEnvVar(name string) int {
	value, exists := readIntEnvVar(name)
	if !exists {
		log.Fatalf("Environment variable %s is not set or invalid", name)
	}
	return value
}
