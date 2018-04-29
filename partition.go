package turing

type PartitionHandler func (partition *Partition, message DecodedKV)
type PartitionCommitHandler func (partition *Partition, message MessageEvent)

const (
	OffsetEarliest = -1
	OffsetLatest = -2
	OffsetStored = -3
	OffsetNone = -4
)

type Partition struct {
	offset int64
	offsetChan chan int64
	closeChan chan struct{}
	handler PartitionHandler
	commitHandler PartitionCommitHandler
	codec Codec

	Topic string
	Id int64
	Messages chan MessageEvent
}

func (p *Partition) handleMessageEvent(msg MessageEvent) {
	defer func() {
		if p.commitHandler != nil {
			p.commitHandler(p, msg)
		}
	}()

	decoded, err := p.codec.Decode(msg.Key, msg.Value)
	if err == nil {
		p.handler(p, decoded)
	}
}

func (p *Partition) Close() {
	close(p.closeChan)
}

func (p *Partition) SetCodec(codec Codec) {
	p.codec = codec
}

func (p *Partition) SetHandler(handler PartitionHandler) {
	p.handler = handler
}

func (p *Partition) SetCommitBehavior(handler PartitionCommitHandler) {
	p.commitHandler = handler
}

func (p *Partition) SetOffset(offset int64) {
	p.offset = offset
}

func (p *Partition) Run() error {
	if p.codec == nil {
		return NoCodecError
	}

	if p.handler == nil {
		return NoHandlerError
	}

	if p.offset == OffsetNone {
		p.offset = OffsetStored
	}

	p.offsetChan <- p.offset

	for {
		select {
		case <- p.closeChan:
			return nil
		case msg := <- p.Messages:
			p.handleMessageEvent(msg)
		}
	}
}

func NewPartition(topic string, partitionId int64) *Partition {
	return &Partition{
		closeChan: make(chan struct{}),
		offset: OffsetNone,
		offsetChan: make(chan int64, 1),
		commitHandler: nil,
		Topic: topic,
		Id: partitionId,
		Messages: make(chan MessageEvent),
	}
}

func DefaultCommitBehavior() PartitionCommitHandler {
	return func (partition *Partition, message MessageEvent) {

	}
}