package main

import "github.com/taciogt/go-kafka/src/topics"

func main() {
	topics.ListTopics()
	topics.GetSchema()

	topicName := "go.kafka.test"
	topics.CreateTopic(topicName)
}
