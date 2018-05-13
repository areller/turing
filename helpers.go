package turing

import (
	"time"
)

func tryWithTimeout(ts time.Duration, what func()) bool {
	done := make(chan struct{})
	go func() {
		what()
		close(done)
	}()

	select {
	case <-time.After(ts):
		return false
	case <-done:
		return true
	}
}

func MustGet(val interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}

	return val
}

func Must(err error) {
	if err != nil {
		panic(err)
	}
}