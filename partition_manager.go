package turing

type PartitionManager struct {
	close chan struct{}
	consumer Consumer
	codecs map[string]Codec
	partitions map[string]*Partition

	CreatedPartition chan *Partition
	RemovedPartition chan *Partition
	Errors chan error
}

func (pm *PartitionManager) handlePartitionEvent(ev PartitionEvent) {
	switch ev.Type {
	case PartitionCreated:
		_, ok := pm.codecs[ev.Topic]
		if ok {
			part := NewPartition(ev.Topic, ev.Id)
			pm.partitions[ev.String()] = part
			pm.CreatedPartition <- part
		} else {
			pm.Errors <- NoCodecForTopic
		}
	case PartitionDestroyed:
		part, ok := pm.partitions[ev.String()]
		if ok {
			delete(pm.partitions, ev.String())
			pm.RemovedPartition <- part
		} else {
			pm.Errors <- NoPartition
		}
	}
}

func (pm *PartitionManager) Close() {
	close(pm.close)
}

func (pm *PartitionManager) SetCodec(topic string, codec Codec) {
	pm.codecs[topic] = codec
}

func (pm *PartitionManager) Run() {
	for {
		select {
		case <- pm.close:
			return
		case ev := <- pm.consumer.PartitionEvent():
			pm.handlePartitionEvent(ev)
		}
	}
}

func NewPartitionManager(consumer Consumer) *PartitionManager {
	return &PartitionManager{
		consumer: consumer,
		codecs: make(map[string]Codec),
		partitions: make(map[string]*Partition),
		CreatedPartition: make(chan *Partition),
		RemovedPartition: make(chan *Partition),
		Errors: make(chan error),
	}
}