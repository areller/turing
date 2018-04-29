package turing

import (
	"time"
)

type Ticker struct {
	closeChan chan struct{}
	tticker *time.Ticker
	todo func()
}

func (t *Ticker) Close() {
	close(t.closeChan)
}

func (t *Ticker) Run() error {
	for {
		select {
		case <-t.closeChan:
			return nil
		case <-t.tticker.C:
			t.todo()
		}
	}
}

func NewTicker(duration time.Duration, todo func ()) *Ticker {
	return &Ticker{
		closeChan: make(chan struct{}),
		tticker: time.NewTicker(duration),
		todo: todo,
	}
}