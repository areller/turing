package confluent

import (
	"sync/atomic"
	"sync"
	"strings"
	"github.com/areller/turing"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ProducerConfig struct {
	Brokers []string
}

func ProducerConfigFromTable(table turing.ConfigTable) ProducerConfig {
	return ProducerConfig{
		Brokers: strings.Split(table.GetString("kafka_brokers"), ","),
	}
}

type Producer struct {
	cproducer *kafka.Producer
	wg sync.WaitGroup
	deliveryChan chan kafka.Event
	closeChan chan struct{}
	msgId int64
	waitMap map[int64]*sync.WaitGroup
}

func (p *Producer) Send(topic string, key []byte, msg []byte) error {
	id := atomic.AddInt64(&p.msgId, 1)
	err := p.cproducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{ Topic: &topic, Partition: kafka.PartitionAny },
		Key: key,
		Value: msg,
		Opaque: id,
	}, p.deliveryChan)

	if err != nil {
		return err
	}

	p.wg.Add(1)
	
	var localWg sync.WaitGroup
	localWg.Add(1)
	p.waitMap[id] = &localWg

	localWg.Wait()
	return err
}

func (p *Producer) Close() {
	p.wg.Wait()
	close(p.closeChan)
}

func (p *Producer) Run() error {
	for {
		select {
		case <-p.closeChan:
			return nil
		case e := <-p.deliveryChan:
			msg := e.(*kafka.Message)
			id := msg.Opaque.(int64)
			localWg := p.waitMap[id]
			delete(p.waitMap, id)
			localWg.Done()
			p.wg.Done()
		}
	}
}

func NewProducer(config ProducerConfig) *Producer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Brokers, ","),
	})

	if err != nil {
		panic(err)
	}

	return &Producer{
		cproducer: p,
		msgId: 0,
		deliveryChan: make(chan kafka.Event),
		closeChan: make(chan struct{}),
		waitMap: make(map[int64]*sync.WaitGroup),
	}
}