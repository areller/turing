package confluent

import (
	"github.com/areller/turing"
	"strings"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type assignment struct {
	topic string
	partition int64
	offset int64
}

type ConsumerConfig struct {
	Brokers []string
	Group string
	AutoCommit bool
	AutoCommitInterval int
}

func ConsumerConfigFromTable(table turing.ConfigTable) ConsumerConfig {
	return ConsumerConfig{
		Brokers: strings.Split(table.GetString("kafka_brokers"), ","),
		Group: table.GetString("kafka_group"),
		AutoCommit: table.GetBool("kafka_auto_commit"),
		AutoCommitInterval: table.GetInt("kafka_auto_commit_interval"),
	}
}

type Consumer struct {
	cconsumer *kafka.Consumer
	topicsMap map[string]*string
	closeChan chan struct{}
	assignChan chan assignment
	partitionEventChan chan turing.PartitionEvent
	messageEventChan chan turing.MessageEvent
}

func (c *Consumer) handleAssignedPartitions(parts kafka.AssignedPartitions) {
	c.assignChan = make(chan assignment, len(parts.Partitions))

	partsMap := make(map[string]map[int64]kafka.TopicPartition)
	for _, p := range parts.Partitions {
		c.topicsMap[*p.Topic] = p.Topic
		if _, ok := partsMap[*p.Topic]; !ok {
			partsMap[*p.Topic] = make(map[int64]kafka.TopicPartition)
		}
		partsMap[*p.Topic][int64(p.Partition)] = p
		c.partitionEventChan <- turing.PartitionEvent{
			Type: turing.PartitionCreated,
			Topic: *p.Topic,
			Id: int64(p.Partition),
		}
	}

	finalParts := make([]kafka.TopicPartition, len(parts.Partitions))
	i := 0
	for i < len(parts.Partitions) {
		ass := <- c.assignChan
		var off kafka.Offset
		
		switch ass.offset {
		case turing.OffsetEarliest:
			off = kafka.OffsetBeginning
		case turing.OffsetLatest:
			off = kafka.OffsetEnd
		case turing.OffsetStored:
			off = kafka.OffsetStored
		case turing.OffsetNone:
			off = kafka.OffsetStored
		default:
			off, _ = kafka.NewOffset(ass.offset)
		}

		finalParts[i] = kafka.TopicPartition{
			Topic: partsMap[ass.topic][ass.partition].Topic,
			Partition: partsMap[ass.topic][ass.partition].Partition,
			Offset: off,
		}

		i++
	}

	c.cconsumer.Assign(finalParts)
}

func (c *Consumer) handleRevokedPartitions(parts kafka.RevokedPartitions) {
	for _, p := range parts.Partitions {
		c.partitionEventChan <- turing.PartitionEvent{
			Type: turing.PartitionDestroyed,
			Topic: *p.Topic,
			Id: int64(p.Partition),
		}
	}

	c.cconsumer.Unassign()
}

func (c *Consumer) handleMessage(msg *kafka.Message) {
	c.messageEventChan <- turing.MessageEvent{
		Topic: *msg.TopicPartition.Topic,
		PartitionId: int64(msg.TopicPartition.Partition),
		Offset: int64(msg.TopicPartition.Offset),
		Key: msg.Key,
		Value: msg.Value,
	}
}

func (c *Consumer) handlePartitionEOF(eof kafka.PartitionEOF) {
	c.partitionEventChan <- turing.PartitionEvent{
		Type: turing.PartitionEnd,
		Topic: *eof.Topic,
		Id: int64(eof.Partition),
	}
}

func (c *Consumer) PartitionEvent() <-chan turing.PartitionEvent {
	return c.partitionEventChan
}

func (c *Consumer) MessageEvent() <-chan turing.MessageEvent {
	return c.messageEventChan
}

func (c *Consumer) Assign(topic string, partition int64, offset int64) {
	c.assignChan <- assignment{
		topic: topic,
		partition: partition,
		offset: offset,
	}
}

func (c *Consumer) Commit(topic string, partition int64, offset int64) {
	off, _ := kafka.NewOffset(offset)

	c.cconsumer.CommitOffsets([]kafka.TopicPartition{
		kafka.TopicPartition{
			Topic: c.topicsMap[topic],
			Partition: int32(partition),
			Offset: off,
		},
	})
}

func (c *Consumer) Subscribe(topics []string) {
	c.cconsumer.SubscribeTopics(topics, nil)
}

func (c *Consumer) Close() {
	close(c.closeChan)
	c.cconsumer.Close()
}

func (c *Consumer) Run() error {
	for {
		select {
		case <- c.closeChan:
			return nil
		case ev := <- c.cconsumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.handleAssignedPartitions(e)
			case kafka.RevokedPartitions:
				c.handleRevokedPartitions(e)
			case *kafka.Message:
				c.handleMessage(e)
			case kafka.PartitionEOF:
				c.handlePartitionEOF(e)
			}
		}
	}
}

func NewConsumer(config ConsumerConfig) *Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Brokers, ","),
		"group.id": config.Group,
		"go.events.channel.enable":        true,
		"enable.auto.commit": config.AutoCommit,
		"auto.commit.interval.ms": config.AutoCommitInterval,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"},
	})

	if err != nil {
		panic(err)
	}

	return &Consumer{
		cconsumer: c,
		topicsMap: make(map[string]*string),
		closeChan: make(chan struct{}),
		partitionEventChan: make(chan turing.PartitionEvent),
		messageEventChan: make(chan turing.MessageEvent),
	}
}