package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"

	"github.com/streamnative/pulsar-admin-go/pkg/utils"

	pulsaradmin "github.com/streamnative/pulsar-admin-go"
)

var topicRegex = regexp.MustCompile(`persistent://(.*?)/(.*?)/`)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cancel()

	pulsarURL := os.Getenv("PULSAR_URL")
	topicName := os.Getenv("TOPIC_NAME")

	log.Printf(
		"Starting with configuration: Pulsar URL: %s, Topic: %s\n",
		pulsarURL, topicName,
	)

	err := run(ctx, pulsarURL, topicName)
	if err != nil {
		log.Fatal(err)
	}
}

func run(_ context.Context, pulsarURL, topic string) error {
	client, err := pulsaradmin.NewClient(&pulsaradmin.Config{
		WebServiceURL: pulsarURL,
	})
	if err != nil {
		return fmt.Errorf("failed to create pulsar admin client: %w", err)
	}

	matches := topicRegex.FindStringSubmatch(topic)
	if len(matches) < 3 {
		return fmt.Errorf("invalid topic name: %s", topic)
	}

	tenant := matches[1]
	namespace := matches[2]

	ns, err := utils.GetNameSpaceName(tenant, namespace)
	if err != nil {
		return fmt.Errorf("failed to get namespace name: %w", err)
	}

	topics, _, err := client.Topics().List(*ns)
	if err != nil {
		return fmt.Errorf("failed to list topics for tenant %q and namespace %q: %w", tenant, namespace, err)
	}

	log.Printf("Found topics: %+v", topics)

	for _, topic := range topics {
		topic, err := utils.GetTopicName(topic)
		if err != nil {
			log.Fatalf("failed to get topic name %q: %v", topic, err)
		}
		if err = client.Topics().Delete(*topic, false, false); err != nil {
			log.Fatalf("failed to delete topic %+v: %v", *topic, err)
		}
	}

	//var (
	//	wg    sync.WaitGroup
	//	guard = make(chan struct{}, 10)
	//)
	//wg.Add(len(partitions))
	//for _, partition := range partitions {
	//	guard <- struct{}{}
	//	go func(partition string) {
	//		defer wg.Done()
	//		topic, err := utils.GetTopicName(partition)
	//		if err != nil {
	//			log.Fatalf("failed to get topic name with partition %q: %v", partition, err)
	//		}
	//		if err = client.Topics().Delete(*topic, false, false); err != nil {
	//			log.Fatalf("failed to delete topic %+v: %v", *topic, err)
	//		}
	//	}(partition)
	//}
	//
	//wg.Wait()

	return nil
}
