package turing

const (
	PartitionCreated = iota
	PartitionDestroyed = iota
)

type PartitionEvent struct {
	Type int
	Topic string
	Id int64
}

type MessageEvent struct {
	Topic string
	PartitionId int64
	Offset int64
	Key []byte
	Value []byte
}

type Consumer interface {
	PartitionEvent() <-chan PartitionEvent
	MessageEvent() <-chan MessageEvent
	Subscribe(topics []string)
}