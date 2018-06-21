package turing

import (
	"github.com/sirupsen/logrus"
	"strconv"
)

type partitionMessageTuple struct {
	p *Partition
	msg MessageEvent
}

type SimpleProcessorContext struct {
	Processor *SimpleProcessor
	Partition *Partition
	TopicObject interface{}
	ProcessorObject interface{}
	Encoded EncodedKV
}

type SimpleProcessorHandler func (context SimpleProcessorContext, msg DecodedKV) (err error, moveOn bool)

type SimpleProcessorTopicDefinition struct {
	Name string
	Codec Codec
	Handler SimpleProcessorHandler
	Object interface{}
}

func (sptd SimpleProcessorTopicDefinition) transformHandler(sp *SimpleProcessor) PartitionHandler {
	return func (p *Partition, original EncodedKV, msg DecodedKV) {
		for {
			err, moveOn := sptd.Handler(SimpleProcessorContext{
				Partition: p,
				TopicObject: sptd.Object,
				Processor: sp,
				ProcessorObject: sp.obj,
				Encoded: original,
			}, msg)

			if err == FatalError {
				Log.WithError(err).WithFields(logrus.Fields{
					"topic": p.Topic,
					"partition": p.Id,
					"offset": p.offset,
				}).Panic("Exiting due to a fatal error")
				return
			} else {
				if err != nil {
					Log.WithError(err).WithFields(logrus.Fields{
						"topic": p.Topic,
						"partition": p.Id,
						"offset": p.offset,
						"moveOn": moveOn,
					}).Error("Could not process message, handler returned a non-fatal error")
				}

				if moveOn {
					break
				}
			}
		}
	}
}

type SimpleProcessor struct {
	closeChan chan struct{}
	pm *PartitionManager
	runnable Runnable
	obj interface{}
	topics map[string]SimpleProcessorTopicDefinition
	commitBehavior func (p *Partition, msg MessageEvent)
	offsetPickBehavior func (p *Partition) int64
	commitChan chan partitionMessageTuple
}

func (sp *SimpleProcessor) handlePartitionCreation(p *Partition) {
	topicDef, ok := sp.topics[p.Topic]
	if !ok {
		return
	}

	p.SetCodec(topicDef.Codec)
	p.SetHandler(topicDef.transformHandler(sp))
	p.SetCommitBehavior(func (p *Partition, msg MessageEvent) {
		Log.WithFields(LogFields{
			"topic": p.Topic,
			"partition": p.Id,
			"offset": msg.Offset,
		}).Info("simple processor: commiting message")

		if sp.commitChan != nil {
			sp.commitChan <- partitionMessageTuple{
				p: p,
				msg: msg,
			}
		} else {
			sp.commitBehavior(p, msg)
		}
	})
	p.SetOffset(sp.offsetPickBehavior(p))

	Log.WithFields(LogFields{
		"topic": p.Topic,
		"partition": p.Id,
	}).Info("simple processor: assigned new partition")

	go p.Run()
}

func (sp *SimpleProcessor) handlePartitionRemoval(p *Partition) {
	Log.WithFields(LogFields{
		"topic": p.Topic,
		"partition": p.Id,
	}).Info("simple processor: partition unassigned")

	p.Close()
}

func (sp *SimpleProcessor) runCommitChan(closeChan chan struct{}) {
	for {
		select {
		case <- closeChan:
			return
		case c := <- sp.commitChan:
			sp.commitBehavior(c.p, c.msg)
		}
	}
}

func (sp *SimpleProcessor) SetObject(obj interface{}) {
	sp.obj = obj
}

func (sp *SimpleProcessor) SetCommitBehavior(behavior func (p *Partition, msg MessageEvent)) {
	sp.commitBehavior = behavior
}

func (sp *SimpleProcessor) SetDefaultCommitBehavior(consumer Consumer) {
	sp.commitBehavior = defaultCommitBehavior(consumer)
}

func (sp *SimpleProcessor) SetAsyncCommitBehavior(behavior func (p *Partition, msg MessageEvent), buffer int) {
	sp.commitChan = make(chan partitionMessageTuple, buffer)
	sp.commitBehavior = behavior
}

func (sp *SimpleProcessor) SetAsyncDefaultCommitBehavior(consumer Consumer, buffer int) {
	sp.commitBehavior = defaultCommitBehavior(consumer)
	sp.commitChan = make(chan partitionMessageTuple, buffer)
}

func (sp *SimpleProcessor) SetKVStoreCommit(store KVStore) {
	sp.commitBehavior = func (p *Partition, msg MessageEvent) {
		err := store.HSet("turing_" + p.Topic, strconv.FormatInt(p.Id, 10), msg.Offset)
		if err == ConnectionDroppedError || UnrecongnizableError(err) {
			Log.WithError(err).Panic("Could not commit offset to key-value store")
			return
		}
	}
}

func (sp *SimpleProcessor) SetAsyncKVStoreCommit(store KVStore, buffer int) {
	sp.SetKVStoreCommit(store)
	sp.commitChan = make(chan partitionMessageTuple, buffer)
}

func (sp *SimpleProcessor) SetOffsetPickBehavior(behavior func (p *Partition) int64) {
	sp.offsetPickBehavior = behavior
}

func (sp *SimpleProcessor) SetKVStoreOffsetPick(store KVStore) {
	sp.offsetPickBehavior = func (p *Partition) int64 {
		off, err := store.HGet("turing_" + p.Topic, strconv.FormatInt(p.Id, 10))
		if err == KeyNotExistsError {
			return OffsetStored
		} else if err == ConnectionDroppedError || UnrecongnizableError(err) {
			Log.WithError(err).Panic("Could not fetch offset from key-value store")
			return OffsetNone
		} else {
			offNum, err := strconv.ParseInt(off, 10, 64)
			if err != nil {
				return OffsetStored
			}
			return offNum + 1
		}
	}
}

func (sp *SimpleProcessor) SetKVStoreBehavior(store KVStore) {
	sp.SetKVStoreCommit(store)
	sp.SetKVStoreOffsetPick(store)
}

func (sp *SimpleProcessor) SetAsyncKVStoreBehavior(store KVStore, buffer int) {
	sp.SetAsyncKVStoreCommit(store, buffer)
	sp.SetKVStoreOffsetPick(store)
}

func (sp *SimpleProcessor) Close() {
	sp.pm.Close()
	if sp.runnable != nil {
		sp.runnable.Close()
	}
	
	close(sp.closeChan)
}

func (sp *SimpleProcessor) Run() error {
	go sp.pm.Run()
	if sp.runnable != nil {
		go sp.runnable.Run()
	}

	var commitCloseChan chan struct{} = nil
	if sp.commitChan != nil {
		commitCloseChan = make(chan struct{})
		go sp.runCommitChan(commitCloseChan)
	}

	for {
		select {
		case <- sp.closeChan:
			if commitCloseChan != nil {
				close(commitCloseChan)
			}
			return nil
		case cp := <- sp.pm.CreatedPartition:
			sp.handlePartitionCreation(cp)
		case rp := <- sp.pm.RemovedPartition:
			sp.handlePartitionRemoval(rp)
		}
	}
}

func covertToTopicsMap(topics []SimpleProcessorTopicDefinition) (map[string]SimpleProcessorTopicDefinition, []string, error) {
	topicsMap := make(map[string]SimpleProcessorTopicDefinition)
	topicsNames := make([]string, len(topics))


	for i, tp := range topics {
		if _, ok := topicsMap[tp.Name]; ok {
			return nil, nil, TopicExistsError
		}

		topicsMap[tp.Name] = tp
		topicsNames[i] = tp.Name
	}

	return topicsMap, topicsNames, nil
}

func defaultCommitBehavior(consumer Consumer) func (p *Partition, msg MessageEvent) {
	return func (p *Partition, msg MessageEvent) {
		consumer.Commit(p.Topic, p.Id, msg.Offset)
	}
}

func defaultOffsetPickBehavior() func (p *Partition) int64 {
	return func (p *Partition) int64 {
		return OffsetStored
	}
}

func NewSimpleProcessor(consumer Consumer, runnable Runnable, topics []SimpleProcessorTopicDefinition) (*SimpleProcessor, error) {
	topicsMap, topicsNames, err := covertToTopicsMap(topics)
	if err != nil {
		return nil, err
	}

	consumer.Subscribe(topicsNames)

	return &SimpleProcessor{
		closeChan: make(chan struct{}),
		pm: NewPartitionManager(consumer),
		runnable: runnable,
		topics: topicsMap,
		commitChan: nil,
		commitBehavior: defaultCommitBehavior(consumer),
		offsetPickBehavior: defaultOffsetPickBehavior(),
	}, nil
}