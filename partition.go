package turing

type Partition struct {
	close chan struct{}

	Topic string
	Id int64
	Messages chan DecodedKV
}

func (p *Partition) Close() {
	close(p.close)
}

func NewPartition(topic string, partitionId int64) *Partition {
	return &Partition{
		Topic: topic,
		Id: partitionId,
		Messages: make(chan DecodedKV),
	}
}