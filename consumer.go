package turing

import (
	"strconv"
)

const (
	PartitionCreated = iota
	PartitionDestroyed = iota
)

type PartitionEvent struct {
	Type int
	Topic string
	Id int64
}

func (pe PartitionEvent) String() string {
	return pe.Topic + "_" + strconv.FormatInt(pe.Id, 10)
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
	Commit(topic string, partition int64, offset int64)
}