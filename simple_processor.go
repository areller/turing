package turing

type partitionMessageTuple struct {
	p *Partition
	msg MessageEvent
}

type SimpleProcessorContext struct {
	Processor *SimpleProcessor
	Topic string
	TopicObject interface{}
	ProcessorObject interface{}
}

type SimpleProcessorHandler func (context SimpleProcessorContext, msg DecodedKV)

type SimpleProcessorTopicDefinition struct {
	Name string
	Codec Codec
	Handler SimpleProcessorHandler
	Object interface{}
}

func (sptd SimpleProcessorTopicDefinition) transformHandler(sp *SimpleProcessor) PartitionHandler {
	return func (p *Partition, msg DecodedKV) {
		sptd.Handler(SimpleProcessorContext{
			Topic: p.Topic,
			TopicObject: sptd.Object,
			Processor: sp,
			ProcessorObject: sp.obj,
		}, msg)
	}
}

type SimpleProcessor struct {
	closeChan chan struct{}
	pm *PartitionManager
	runnable Runnable
	obj interface{}
	topics map[string]SimpleProcessorTopicDefinition
	commitBehavior func (p *Partition, msg MessageEvent)
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
	p.SetOffset(OffsetStored)

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
	}, nil
}