package turing

type PartitionedMessage struct {
	Key []byte
	Value []byte
}

type Partition struct {
	close chan struct{}

	Topic string
	Id int64
	Messages chan PartitionedMessage
}

func (p *Partition) Close() {
	close(p.close)
}

func NewPartition(topic string, partitionId int64) *Partition {
	return &Partition{
		Topic: topic,
		Id: partitionId,
		Messages: make(chan PartitionedMessage),
	}
}