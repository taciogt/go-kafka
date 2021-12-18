package topics

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
	"log"
	"os"
	"regexp"
	"time"
)

func ListTopics() {
	cfgMap := kafka.ConfigMap{"bootstrap.servers": "localhost"}
	adminClient, err := kafka.NewAdminClient(&cfgMap)
	if err != nil {
		log.Fatalf("failed to create the kafka admin client. confgis: %v", cfgMap)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dur := 20 * time.Second

	// Ask cluster for the resource's current configuration
	resourceType := kafka.ResourceTopic
	resourceName := "users"
	results, err := adminClient.DescribeConfigs(ctx,
		[]kafka.ConfigResource{{Type: resourceType, Name: resourceName}},
		kafka.SetAdminRequestTimeout(dur))
	if err != nil {
		fmt.Printf("Failed to DescribeConfigs(%s, %s): %s\n",
			resourceType, resourceName, err)
		os.Exit(1)
	}

	// Print results
	for _, result := range results {
		fmt.Printf("%s %s: %s:\n", result.Type, result.Name, result.Error)
		for _, entry := range result.Config {
			// Truncate the value to 60 chars, if needed, for nicer formatting.
			fmt.Printf("%60s = %-60.60s   %-20s Read-only:%v Sensitive:%v\n",
				entry.Name, entry.Value, entry.Source,
				entry.IsReadOnly, entry.IsSensitive)
		}
	}

	metadata, err := adminClient.GetMetadata(nil, true, 1000)
	if err != nil {
		log.Fatalf("Failed to GetMetadata")
	}
	//fmt.Printf("%v", metadata)
	fmt.Printf("topics metadata\n")
	fmt.Printf("%20s\n", "name")
	for k, t := range metadata.Topics {
		//if strings.Contains(k, "confluent") {
		if regexp.MustCompile(`confluent|docker`).MatchString(k) {
			continue
		}
		//fmt.Printf("%10s, %v\n", t.Topic, t)
		fmt.Printf("%20s - %s\n", k, t.Topic)
	}

	adminClient.Close()
}

func CreateTopic(name string) {

}

type ComplexType struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func GetSchema() {
	//topicName := "go-kafka"
	topicName := "com.tacio.gokafka"
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")
	schema, err := schemaRegistryClient.GetLatestSchema(topicName)
	if err != nil {
		log.Fatalf("failed to get latest schema for topic %s. err: %v", topicName, err)
	}
	fmt.Printf("%v\n", schema)
}
