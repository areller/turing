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

	tick := NewTicker(duration, func () {
		rChan <- time.Now()
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