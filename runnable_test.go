package turing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type annonymousRunnable struct {
	close func()
	run func() error
}

func (ar *annonymousRunnable) Close() {
	ar.close()
}

func (ar *annonymousRunnable) Run() error {
	return ar.run()
}

func newAnnonymousRunner(run func() error, close func()) *annonymousRunnable {
	return &annonymousRunnable{
		close: close,
		run: run,
	}
}

func TestCompositeRunnable(t *testing.T) {
	run1Run := false
	run2Run := false

	run1 := newAnnonymousRunner(func () error {
		run1Run = true
		return nil
	}, func () {
		run1Run = false
	})

	run2 := newAnnonymousRunner(func () error {
		run2Run = true
		return nil
	}, func () {
		run2Run = false
	})

	composite := NewCompositeRunnable([]Runnable{ run1, run2 })
	err := composite.Run()
	
	assert.Nil(t,  err)
	assert.True(t, run1Run)
	assert.True(t, run2Run)

	composite.Close()
	
	assert.False(t, run1Run)
	assert.False(t, run2Run)
}