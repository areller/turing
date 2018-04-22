package turing

import (
	"github.com/Shopify/sarama"
)

type SaramaProducer struct {
	producer sarama.SyncProducer
}

func (sp *SaramaProducer) Send(topic string, key []byte, value []byte) error {
	_, _, err := sp.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key: sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	})

	return err
}

func NewSaramaProducer(addresses []string, config *sarama.Config) *SaramaProducer {
	if config == nil {
		config = sarama.NewConfig()
	}

	p, err := sarama.NewSyncProducer(addresses, config)
	if err != nil {
		return nil
	}

	return &SaramaProducer{
		producer: p,
	}
}