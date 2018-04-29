package turing

import (
	"os"
	"os/signal"
	"sync"
	"errors"
	"github.com/tevino/abool"
)

type Runnable interface {
	Close()
	Run() error
}

type CompositeRunnable struct {
	runnables []Runnable
	Errors chan error
}

func (cr *CompositeRunnable) Close() {
	var wg sync.WaitGroup
	for _, r := range cr.runnables {
		wg.Add(1)
		go func(run Runnable) {
			run.Close()
			wg.Done()
		}(r)
	}

	wg.Wait()
}

func (cr *CompositeRunnable) Run() error {
	var wg sync.WaitGroup
	allErrors := abool.NewBool(true)

	for _, r := range cr.runnables {
		wg.Add(1)
		go func(run Runnable) {
			err := run.Run()
			if err != nil {
				cr.Errors <- err
			} else {
				allErrors.SetTo(false)
			}
			wg.Done()
		}(r)
	}

	wg.Wait()
	if allErrors.IsSet() {
		return errors.New("All runners returned an error")
	}

	return nil
}

func NewCompositeRunnable(runnables []Runnable) *CompositeRunnable {
	return &CompositeRunnable{
		runnables: runnables,
		Errors: make(chan error, len(runnables)),
	}
}

func RunInProcess(runnable Runnable) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go runnable.Run()
	for {
		select {
		case <- sigChan:
			runnable.Close()
			return
		}
	}
}

func RunCompositeInProcess(runnables ...Runnable) {
	RunInProcess(NewCompositeRunnable(runnables))
}