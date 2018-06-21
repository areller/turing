package turing

import (
	"time"
)

type Ticker struct {
	closeChan chan struct{}
	tticker *time.Ticker
	todo func(obj interface{}) (err error, moveOn bool)
	obj interface{}
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
			{
				for {
					err, moveOn := t.todo(t.obj)
					if err == FatalError {
						Log.WithError(err).Panic("Exiting due to a fatal error")
						return nil
					} else {
						if err != nil {
							Log.WithError(err).Error("Could not process message, handler returned a non-fatal error")
						}

						if moveOn {
							break
						}
					}
				}
			}
		}
	}
}

func NewTicker(duration time.Duration, object interface{}, todo func (obj interface{}) (error, bool)) *Ticker {
	return &Ticker{
		closeChan: make(chan struct{}),
		tticker: time.NewTicker(duration),
		todo: todo,
		obj: object,
	}
}