package turing

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestPartitionChannel(t *testing.T) {
	con := NewConsumerMock()
	pm := NewPartitionManager(con)
	go pm.Run()

	con.CreatePartitionEvent(PartitionEvent{
		Type: PartitionDestroyed,
		Topic: "myTopic",
		Id: 0,
	})

	pm.SetCodec("myTopic", new(StringCodec))

	select {
	case err := <- pm.Errors:
		assert.Equal(t, NoPartition.Error(), err.Error())
	case <- pm.RemovedPartition:
		t.Error("Unexpected removed partition event")
	case <- pm.CreatedPartition:
		t.Error("Unexpected create partition event")
	}

	con.CreatePartitionEvent(PartitionEvent{
		Type: PartitionCreated,
		Topic: "myTopic",
		Id: 0,
	})

	var part *Partition
	select {
	case part = <- pm.CreatedPartition:
		;
	case err := <- pm.Errors:
		assert.Error(t, err)
	}

	assert.Equal(t, struct {
		Topic string
		Id int64
	}{
		"myTopic",
		0,
	}, struct {
		Topic string
		Id int64
	}{
		part.Topic,
		part.Id,
	})

	con.CreatePartitionEvent(PartitionEvent{
		Type: PartitionCreated,
		Topic: "myTopic",
		Id: 1,
	})

	select {
	case part = <- pm.CreatedPartition:
		;
	case err := <- pm.Errors:
		assert.Error(t, err)
	}

	assert.Equal(t, struct {
		Topic string
		Id int64
	}{
		"myTopic",
		1,
	}, struct {
		Topic string
		Id int64
	}{
		part.Topic,
		part.Id,
	})
}

func TestShouldNotCreateWithoutCodec(t *testing.T) {
	con := NewConsumerMock()
	pm := NewPartitionManager(con)
	go pm.Run()

	pm.SetCodec("myTopic", new(StringCodec))

	con.CreatePartitionEvent(PartitionEvent{
		Type: PartitionCreated,
		Topic: "myTopic2",
		Id: 0,
	})

	select {
	case <- pm.CreatedPartition:
		t.Error("Should not create partition")
	case err := <- pm.Errors:
		assert.Equal(t, NoCodecForTopic.Error(), err.Error())
	}

	con.CreatePartitionEvent(PartitionEvent{
		Type: PartitionCreated,
		Topic: "myTopic",
		Id: 0,
	})

	select {
	case part := <- pm.CreatedPartition:
		assert.Equal(t, struct{
			Topic string
			Id int64
		}{
			"myTopic",
			0,
		}, struct {
			Topic string
			Id int64
		}{
			part.Topic,
			part.Id,
		})
	case <- pm.Errors:
		t.Error("Should not throw an error")
	}
}