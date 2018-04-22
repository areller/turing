package turing

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

type SaramaConsumer struct {
	closeChan chan struct{}
	consumer *cluster.Consumer
}

func (sc *SaramaConsumer) Close() {

}

func (sc *SaramaConsumer) Run() {

}

func NewSaramaConsumer(addresses []string, group string, topics []string) *SaramaConsumer {
	config := cluster.NewConfig()
	config.Group.Mode = cluster.ConsumerModePartitions

	c, err := cluster.NewConsumer(addresses, group, topics, config)
	if err != nil {
		return nil
	}

	return &SaramaConsumer{
		consumer: c,
	}
}