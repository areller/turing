package turing

import (
	"github.com/stretchr/testify/mock"
)

type ConsumerMock struct {
	mock.Mock
	partitionEvent chan PartitionEvent
	messageEvent chan MessageEvent
}

func (cm *ConsumerMock) PartitionEvent() <-chan PartitionEvent {
	return cm.partitionEvent
}

func (cm *ConsumerMock) MessageEvent() <-chan MessageEvent {
	return cm.messageEvent
}

func (cm *ConsumerMock) Subscribe(topics []string) {

}

func (cm *ConsumerMock) Commit(topic string, partition int64, offset int64) {

}

func (cm *ConsumerMock) Assign(topic string, partition int64, offset int64) {

}

func (cm *ConsumerMock) CreatePartitionEvent(pe PartitionEvent) {
	cm.partitionEvent <- pe
}

func (cm *ConsumerMock) CreateMessageEvent(me MessageEvent) {
	cm.messageEvent <- me
}

func NewConsumerMock() *ConsumerMock {
	m := new(ConsumerMock)
	m.partitionEvent = make(chan PartitionEvent, 1)
	m.messageEvent = make(chan MessageEvent, 1)
	return m
}