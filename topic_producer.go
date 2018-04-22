package turing

type TopicProducer struct {
	producer Producer
	codec Codec

	Topic string
}

func (tp *TopicProducer) Send(key string, msg interface{}) error {
	encoded, err := tp.codec.Encode(key, msg)
	if err != nil {
		return err
	}

	err = tp.producer.Send(tp.Topic, encoded.Key, encoded.Value)
	return nil
}

func NewTopicProducer(topic string, codec Codec, producer Producer) *TopicProducer {
	return &TopicProducer{
		producer: producer,
		codec: codec,
		Topic: topic,
	}
}