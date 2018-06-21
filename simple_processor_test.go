package turing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFailToDuplicateTopics(t *testing.T) {
	consumer := NewConsumerMock()

	sp, err := NewSimpleProcessor(consumer,
								  nil,
								  []SimpleProcessorTopicDefinition{
									  SimpleProcessorTopicDefinition{
										  Name: "myTopicA",
										  Codec: new(StringCodec),
										  Handler: func (ctx SimpleProcessorContext, msg DecodedKV) (error, bool) {
											  return nil, true
										  },
									  },
									  SimpleProcessorTopicDefinition{
										  Name: "myTopicB",
										  Codec: new(StringCodec),
										  Handler: func (ctx SimpleProcessorContext, msg DecodedKV) (error, bool) {
											  return nil, true
										  },
									  },
								  })

	assert.NotNil(t, sp)
	assert.Nil(t, err)

	sp, err = NewSimpleProcessor(consumer,
							     nil,
								  []SimpleProcessorTopicDefinition{
									  SimpleProcessorTopicDefinition{
										  Name: "myTopicA",
										  Codec: new(StringCodec),
										  Handler: func (ctx SimpleProcessorContext, msg DecodedKV) (error, bool) {
											  return nil, true
										  },
									  },
									  SimpleProcessorTopicDefinition{
										  Name: "myTopicA",
										  Codec: new(StringCodec),
										  Handler: func (ctx SimpleProcessorContext, msg DecodedKV) (error, bool) {
											  return nil, true
										  },
									  },
								  })

	assert.Nil(t, sp)
	assert.Equal(t, TopicExistsError, err)
}

func TestMessageProcessing(t *testing.T) {
	chanA := make(chan DecodedKV, 1)
	chanB := make(chan DecodedKV, 1)
	var sp *SimpleProcessor
	tMap := map[int]string{
		0: "topicA",
		1: "topicB",
	}

	handler := func (topic int) func (ctx SimpleProcessorContext, msg DecodedKV) (error, bool) {
		return func (ctx SimpleProcessorContext, msg DecodedKV) (error, bool) {
			assert.Equal(t, sp, ctx.Processor)
			assert.Equal(t, topic, ctx.TopicObject.(int))
			assert.Equal(t, "processorobject", ctx.ProcessorObject.(string))
			assert.Equal(t, tMap[topic], ctx.Partition.Topic)

			switch topic {
			case 0:
				chanA <- msg
			case 1:
				chanB <- msg
			}

			return nil, true
		}
	}

	consumer := NewConsumerMock()
	sp, _ = NewSimpleProcessor(consumer, nil, []SimpleProcessorTopicDefinition{
		SimpleProcessorTopicDefinition{
			Name: tMap[0],
			Codec: new(StringCodec),
			Handler: handler(0),
			Object: 0,
		},
		SimpleProcessorTopicDefinition{
			Name: tMap[1],
			Codec: new(StringCodec),
			Handler: handler(1),
			Object: 1,
		},
	})

	sp.SetObject("processorobject")

	go sp.Run()
	consumer.CreatePartitionEvent(PartitionEvent{
		Type: PartitionCreated,
		Topic: "topicA",
		Id: 0,
	})
	consumer.CreatePartitionEvent(PartitionEvent{
		Type: PartitionCreated,
		Topic: "topicA",
		Id: 1,
	})
	consumer.CreatePartitionEvent(PartitionEvent{
		Type: PartitionCreated,
		Topic: "topicB",
		Id: 0,
	})

	consumer.CreateMessageEvent(MessageEvent{
		Topic: tMap[0],
		PartitionId: 0,
		Offset: 0,
		Key: []byte("key00"),
		Value: []byte("value00"),
	})

	msg := <- chanA
	assert.Equal(t, "key00", msg.Key)
	assert.Equal(t, "value00", msg.Value.(string))
}