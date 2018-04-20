package turing

import (
	"time"
)

func doWithTimeout(timeout time.Duration, action func ()) bool {
	done := make(chan struct{})
	go func () {
		action()
		close(done)
	}()

	select {
	case <- done:
		return true
	case <- time.After(timeout):
		return false
	}
}