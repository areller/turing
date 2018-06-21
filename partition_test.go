package turing

import (
	"time"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestReturnErrorIfNoCodecOrHandler(t *testing.T) {
	part := NewPartition("myTopic", 0)
	part.SetHandler(func (p *Partition, original EncodedKV, msg DecodedKV) {

	})

	done := make(chan struct{})
	go func() {
		err := part.Run()
		assert.Equal(t, NoCodecError, err)
		close(done)
	}()

	select {
	case <- time.After(time.Second):
		t.Error("Timeout")
	case <- done:
		;
	}

	part = NewPartition("myTopic", 0)
	part.SetCodec(new(StringCodec))

	done = make(chan struct{})
	go func() {
		err := part.Run()
		assert.Equal(t, NoHandlerError, err)
		close(done)
	}()

	select {
	case <- time.After(time.Second):
		t.Error("Timeout")
	case <- done:
		;
	}

	part.SetHandler(func (p *Partition, original EncodedKV, msg DecodedKV) {

	})

	done = make(chan struct{})
	go func() {
		err := part.Run()
		assert.Equal(t, nil, err)
		close(done)
	}()

	part.Close()
	<- done
}

func TestMessageDecoding(t *testing.T) {
	part := NewPartition("myTopic", 0)
	part.SetCodec(new(StringCodec))

	done := make(chan struct{})
	part.SetHandler(func (p *Partition, original EncodedKV, msg DecodedKV) {
		assert.Equal(t, "myKey", msg.Key)
		assert.Equal(t, "My Message", msg.Value)
		assert.Equal(t, []byte("myKey"), original.Key)
		assert.Equal(t, []byte("My Message"), original.Value)
		close(done)
	})

	go func() {
		err := part.Run()
		assert.Equal(t, nil, err)
	}()

	part.Messages <- MessageEvent{
		Topic: "myTopic",
		PartitionId: 0,
		Offset: 0,
		Key: []byte("myKey"),
		Value: []byte("My Message"),
	}

	<- done
}

func TestCommitHandler(t *testing.T) {
	part := NewPartition("myTopic", 0)
	part.SetCodec(new(StringCodec))

	done := make(chan struct{})
	part.SetHandler(func (p *Partition, original EncodedKV, msg DecodedKV) {
		assert.Equal(t, "myKey", msg.Key)
		assert.Equal(t, "My Message", msg.Value)
		assert.Equal(t, []byte("myKey"), original.Key)
		assert.Equal(t, []byte("My Message"), original.Value)
		close(done)
	})

	done2 := make(chan struct{})
	part.SetCommitBehavior(func (p *Partition, msg MessageEvent) {
		assert.Equal(t, part.Id, p.Id)
		assert.EqualValues(t, 0, msg.PartitionId)
		assert.EqualValues(t, 0, msg.Offset)
		assert.EqualValues(t, "myTopic", msg.Topic)
		close(done2)
	})

	go func() {
		err := part.Run()
		assert.Equal(t, nil, err)
	}()

	part.Messages <- MessageEvent{
		Topic: "myTopic",
		PartitionId: 0,
		Offset: 0,
		Key: []byte("myKey"),
		Value: []byte("My Message"),
	}

	<- done
	<- done2
}

func TestPartitionAssign(t *testing.T) {
	getOffset := func (cb func (p *Partition)) int64 {
		part := NewPartition("myTopic", 0)
		part.SetCodec(new(StringCodec))
		part.SetHandler(func (p *Partition, original EncodedKV, msg DecodedKV) {

		})

		cb(part)

		done := make(chan struct{})
		go func() {
			err := part.Run()
			assert.Equal(t, nil, err)
			close(done)
		}()

		defer func() {
			part.Close()
			<- done
		}()

		off := <- part.offsetChan
		return off
	}

	assert.EqualValues(t, OffsetStored, getOffset(func (p *Partition) {}))
	assert.EqualValues(t, OffsetEarliest, getOffset(func (p *Partition) {
		p.SetOffset(OffsetEarliest)
	}))
	assert.EqualValues(t, OffsetLatest, getOffset(func (p *Partition) {
		p.SetOffset(OffsetLatest)
	}))
	assert.EqualValues(t, 5, getOffset(func (p *Partition) {
		p.SetOffset(5)
	}))
}