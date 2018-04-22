package turing

type producerMockMessage struct {
	topic string
	key []byte
	value []byte
}

type ProducerMock struct {
	SentMessages chan producerMockMessage
}

func (pm *ProducerMock) Send(topic string, key []byte, msg []byte) error {
	pm.SentMessages <- producerMockMessage{
		topic: topic,
		key: key,
		value: msg,
	}

	return nil
}

func NewProducerMock() *ProducerMock {
	return &ProducerMock{
		SentMessages: make(chan producerMockMessage, 1),
	}
}