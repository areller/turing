package main

import (
	"fmt"
	"github.com/areller/turing"
	"github.com/areller/turing/confluent"
)

func HandleTestMessages(msg turing.DecodedKV) {
	val := msg.Value.(string)
	fmt.Println("New Message %s: %s", msg.Key, val)
}

func main() {
	consumer := confluent.NewConsumer(confluent.ConsumerConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Group: "sGroup",
	})
	simpleProcessor, _ := turing.NewSimpleProcessor(consumer,
													consumer,
												    []turing.SimpleProcessorTopicDefinition{
														turing.SimpleProcessorTopicDefinition{
															Name: "topicZ",
															Codec: new(turing.StringCodec),
															Handler: HandleTestMessages,
														},
													})

	turing.RunInProcess(simpleProcessor)
}