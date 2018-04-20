package turing

type PartitionHandler func (partition *Partition, message DecodedKV)

type Partition struct {
	close chan struct{}
	handler PartitionHandler

	Topic string
	Id int64
	Messages chan DecodedKV
}

func (p *Partition) handleMessageEvent(msg DecodedKV) {
	if p.handler != nil {
		p.handler(p, msg)
	}
}

func (p *Partition) Close() {
	close(p.close)
}

func (p *Partition) SetHandler(handler PartitionHandler) {
	p.handler = handler
}

func (p *Partition) Run() {
	for {
		select {
		case <- p.close:
			return
		case msg := <- p.Messages:
			p.handleMessageEvent(msg)
		}
	}
}

func NewPartition(topic string, partitionId int64) *Partition {
	return &Partition{
		Topic: topic,
		Id: partitionId,
		Messages: make(chan DecodedKV),
	}
}