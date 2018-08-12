package main

import (
	"fmt"
	"github.com/areller/turing"
	"github.com/areller/turing/confluent"
)

func HandleTestMessages(context turing.SimpleProcessorContext, msg turing.DecodedKV) (error, bool) {
	val := msg.Value.(string)
	fmt.Println("New Message %s: %s", msg.Key, val)
	return nil, true
}

func main() {
	consumer := confluent.NewConsumer(confluent.ConsumerConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Group: "sGroup",
		LogConnectionClose: false,
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