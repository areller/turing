package main

import (
	"fmt"
	"time"
	"github.com/areller/turing"
	"github.com/areller/turing/confluent"
)

func main() {
	prod := confluent.NewProducer([]string{"127.0.0.1:9092"})
	topicZProd := turing.NewTopicProducer(
		"topicZ",
		new(turing.StringCodec),
		prod,
	)

	turing.RunCompositeInProcess(prod, turing.NewTicker(time.Second, func () {
		fmt.Println("Sending")
		topicZProd.Send("Arik", "Message Arik")
		fmt.Println("Sent")
	}))
}