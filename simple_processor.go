package turing

type SimpleProcessorHandler func (msg DecodedKV)

type SimpleProcessorTopicDefinition struct {
	Name string
	Codec Codec
	Handler SimpleProcessorHandler
}

func (sptd SimpleProcessorTopicDefinition) transformHandler() PartitionHandler {
	return func (p *Partition, msg DecodedKV) {
		sptd.Handler(msg)
	}
}

type SimpleProcessor struct {
	closeChan chan struct{}
	pm *PartitionManager
	runnable Runnable
	topics map[string]SimpleProcessorTopicDefinition
}

func (sp *SimpleProcessor) handlePartitionCreation(p *Partition) {
	topicDef, ok := sp.topics[p.Topic]
	if !ok {
		return
	}

	p.SetCodec(topicDef.Codec)
	p.SetHandler(func (p *Partition, msg DecodedKV) {
		topicDef.Handler(msg)
	})
	p.SetCommitBehavior(func (p *Partition, msg MessageEvent) {
		sp.pm.consumer.Commit(p.Topic, p.Id, msg.Offset)
	})
	p.SetOffset(OffsetStored)

	go p.Run()
}

func (sp *SimpleProcessor) handlePartitionRemoval(p *Partition) {
	p.Close()
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

	for {
		select {
		case <- sp.closeChan:
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
	}, nil
}