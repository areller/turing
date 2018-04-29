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
										  Handler: func (msg DecodedKV) {},
									  },
									  SimpleProcessorTopicDefinition{
										  Name: "myTopicB",
										  Codec: new(StringCodec),
										  Handler: func (msg DecodedKV) {},
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
										  Handler: func (msg DecodedKV) {},
									  },
									  SimpleProcessorTopicDefinition{
										  Name: "myTopicA",
										  Codec: new(StringCodec),
										  Handler: func (msg DecodedKV) {},
									  },
								  })

	assert.Nil(t, sp)
	assert.Equal(t, TopicExistsError, err)
}