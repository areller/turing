package turing

import (
	"github.com/stretchr/testify/mock"
)

type ConsumerMock struct {
	mock.Mock
	partitionEvent chan PartitionEvent
}

func (cm *ConsumerMock) PartitionEvent() <-chan PartitionEvent {
	return cm.partitionEvent
}

func (cm *ConsumerMock) MessageEvent() <-chan MessageEvent {
	return cm.Called().Get(0).(<-chan MessageEvent)
}

func (cm *ConsumerMock) Subscribe(topics []string) {
	cm.Called()
}

func (cm *ConsumerMock) Commit(topic string, partition int64, offset int64) {
	
}

func (cm *ConsumerMock) CreatePartitionEvent(pe PartitionEvent) {
	cm.partitionEvent <- pe
}

func NewConsumerMock() *ConsumerMock {
	m := new(ConsumerMock)
	m.partitionEvent = make(chan PartitionEvent, 1)
	return m
}