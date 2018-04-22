package turing

import (
	"strconv"
)

const (
	PartitionCreated = iota
	PartitionDestroyed = iota
	PartitionEnd = iota
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

func (me MessageEvent) PartitionString() string {
	return me.Topic + "_" + strconv.FormatInt(me.PartitionId, 10)
}

type Consumer interface {
	PartitionEvent() <-chan PartitionEvent
	MessageEvent() <-chan MessageEvent
	Commit(topic string, partition int64, offset int64)
	Assign(topic string, partition int64, offset int64)
}