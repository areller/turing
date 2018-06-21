package tester

import (
	"github.com/areller/turing"
)

type activePartition struct {
	id int
	currentOffset int64
}

type activeTopic struct {
	totalMessages int64
	partitions map[int]*activePartition
	numPartitions int
	def TopicDescription
}

type TopicDescription struct {
	Name string
	Partitions int
	Codec turing.Codec
}

type ConsumerTester struct {
	topics map[string]TopicDescription
	partitionsChan chan turing.PartitionEvent
	messagesChan chan turing.MessageEvent
	activeTopics map[string]*activeTopic
}

func (ct *ConsumerTester) Assign(topic string, partition int64, offset int64) { }

func (ct *ConsumerTester) Commit(topic string, partition int64, offset int64) { }

func (ct *ConsumerTester) Subscribe(topics []string) {
	for _, topicName := range topics {
		definedTopic, ok := ct.topics[topicName]
		if !ok {
			continue
		}

		partitions := definedTopic.Partitions
		ct.activeTopics[topicName] = &activeTopic{
			totalMessages: 0,
			partitions: make(map[int]*activePartition),
			numPartitions: partitions,
			def: definedTopic,
		}

		for i := 0; i < partitions; i++ {
			ct.activeTopics[topicName].partitions[i] = &activePartition{
				id: i,
				currentOffset: 0,
			}
			ct.partitionsChan <- turing.PartitionEvent{
				Type: turing.PartitionCreated,
				Topic: topicName,
				Id: int64(i),
			}
		}
	}
}

func (ct *ConsumerTester) PartitionEvent() <-chan turing.PartitionEvent {
	return ct.partitionsChan
}

func (ct *ConsumerTester) MessageEvent() <-chan turing.MessageEvent {
	return ct.messagesChan
}

func (ct *ConsumerTester) SendMessage(topic string, key string, message interface{}) error {
	activeTopic, ok := ct.activeTopics[topic]
	if !ok {
		return turing.TopicNotExistsError
	}

	encoded, err := activeTopic.def.Codec.Encode(key, message)
	if err != nil {
		return err
	}

	part := activeTopic.partitions[int(activeTopic.totalMessages) % activeTopic.numPartitions]
	ct.messagesChan <- turing.MessageEvent{
		Topic: topic,
		PartitionId: int64(part.id),
		Offset: part.currentOffset,
		Key: encoded.Key,
		Value: encoded.Value,
	}

	activeTopic.totalMessages++
	part.currentOffset++
	return nil
}

func NewConsumerTester(topics []TopicDescription) *ConsumerTester {
	topicsMap := make(map[string]TopicDescription)
	buffer := 0

	for _, topic := range topics {
		topicsMap[topic.Name] = topic
		buffer += topic.Partitions
	}

	return &ConsumerTester{
		topics: topicsMap,
		partitionsChan: make(chan turing.PartitionEvent, buffer),
		messagesChan: make(chan turing.MessageEvent, 1),
		activeTopics: make(map[string]*activeTopic),
	}
}