package turing

type Producer interface {
	Send(topic string, key []byte, msg []byte) error
}