package turing

import (
	"errors"
)

var (
	TopicExistsError = errors.New("Topic already exists")
	InvalidTypeError = errors.New("Object is of invalid type")
	NoCodecError = errors.New("No codec is defined")
	NoHandlerError = errors.New("No handler is defined")
	NoPartitionError = errors.New("No such partition is defined")
	WrongTypeError = errors.New("Wrong type")
	KeyNotExistsError = errors.New("Key does not exist")
	GeneralError = errors.New("General error")
)