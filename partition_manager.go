package turing

type PartitionManager struct {
	consumer *Consumer
	decoders map[string]*Decoder
}

func (pm *PartitionManager) AddDecoder(topic string, decoder *Decoder) error {
	_, ok := pm.decoders[topic]
	if ok {
		return TopicExists
	}

	pm.decoders[topic] = decoder
	return nil
}

func NewPartitionManager(consumer *Consumer) *PartitionManager {
	return &PartitionManager{
		consumer: consumer,
		decoders: make(map[string]*Decoder),
	}
}