package turing

import (
	_ "math"
	"github.com/stretchr/testify/assert"
	"time"
	"testing"
)

func TestTicking(t *testing.T) {
	rChan := make(chan time.Time)
	duration := 500 * time.Millisecond

	tick := NewTicker(duration, nil, func (obj interface{}) (error, bool) {
		rChan <- time.Now()
		return nil, true
	})

	go tick.Run()
	var pTime time.Time
	for i := 0; i < 5; i++ {
		currTime := <- rChan
		if i > 0 {
			assert.InEpsilon(t, duration.Nanoseconds(), currTime.Sub(pTime).Nanoseconds(), 0.1)
		}
		pTime = currTime
	}

	tick.Close()
	res := tryWithTimeout(time.Second, func () {
		<- rChan
	})

	assert.False(t, res)
}

func TestTickerObject(t *testing.T) {
	object := "AB"
	ch := make(chan interface{})
	tick := NewTicker(time.Millisecond, object, func (obj interface{}) (error, bool) {
		ch <- obj
		return nil, true
	})

	go tick.Run()
	var matchingObject interface{}
	res := tryWithTimeout(time.Second, func () {
		matchingObject = <- ch
	})

	assert.True(t, res)
	assert.Equal(t, object, matchingObject)
}