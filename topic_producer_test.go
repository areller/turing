package turing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProducing(t *testing.T) {
	pm := NewProducerMock()
	tp := NewTopicProducer("myTopic", new(StringCodec), pm)

	tp.Send("myKey", "My Message")
	msg := <- pm.SentMessages

	assert.Equal(t, "myTopic", msg.topic)
	assert.Equal(t, []byte("myKey"), msg.key)
	assert.Equal(t, []byte("My Message"), msg.value)
}