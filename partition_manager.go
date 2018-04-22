package turing

type partitionRunner struct {
	manager *PartitionManager
	partition *Partition
	closeChan chan struct{}
}

func (runner *partitionRunner) close() {
	close(runner.closeChan)
}

func (runner *partitionRunner) run() {
	for {
		select {
		case <- runner.closeChan:
			return
		case off := <- runner.partition.offsetChan:
			runner.manager.consumer.Assign(runner.partition.Topic, runner.partition.Id, off)
		}
	}
}

type PartitionManager struct {
	closeChan chan struct{}
	consumer Consumer
	partitions map[string]*partitionRunner

	CreatedPartition chan *Partition
	RemovedPartition chan *Partition
	Errors chan error
}

func (pm *PartitionManager) handlePartitionEvent(ev PartitionEvent) {
	switch ev.Type {
	case PartitionCreated:
		part := NewPartition(ev.Topic, ev.Id)
		id := ev.String()
		pm.partitions[id] = &partitionRunner{
			manager: pm,
			partition: part,
			closeChan: make(chan struct{}),
		}
		go pm.partitions[id].run()
		pm.CreatedPartition <- part
	case PartitionDestroyed:
		part, ok := pm.partitions[ev.String()]
		if ok {
			delete(pm.partitions, ev.String())
			part.close()
			pm.RemovedPartition <- part.partition
		} else {
			pm.Errors <- NoPartitionError
		}
	}
}

func (pm *PartitionManager) handleMessageEvent(ev MessageEvent) {
	part, ok := pm.partitions[ev.PartitionString()]
	if !ok {
		return
	}

	part.partition.Messages <- ev
}

func (pm *PartitionManager) Close() {
	close(pm.closeChan)
}

func (pm *PartitionManager) Run() {
	for {
		select {
		case <- pm.closeChan:
			return
		case ev := <- pm.consumer.PartitionEvent():
			pm.handlePartitionEvent(ev)
		case ev := <- pm.consumer.MessageEvent():
			pm.handleMessageEvent(ev)
		}
	}
}

func NewPartitionManager(consumer Consumer) *PartitionManager {
	return &PartitionManager{
		closeChan: make(chan struct{}),
		consumer: consumer,
		partitions: make(map[string]*partitionRunner),
		CreatedPartition: make(chan *Partition),
		RemovedPartition: make(chan *Partition),
		Errors: make(chan error, 10),
	}
}