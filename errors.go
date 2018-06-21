package turing

import (
	"errors"
)

var (
	TopicExistsError = errors.New("Topic already exists")
	TopicNotExistsError = errors.New("Topic does not exist")
	InvalidTypeError = errors.New("Object is of invalid type")
	NoCodecError = errors.New("No codec is defined")
	NoHandlerError = errors.New("No handler is defined")
	NoPartitionError = errors.New("No such partition is defined")
	WrongTypeError = errors.New("Wrong type")
	KeyNotExistsError = errors.New("Key does not exist")
	GeneralError = errors.New("General error")
	ConnectionDroppedError = errors.New("Connection dropped")
	FatalError = errors.New("Fatal error")
)

func UnrecongnizableError(err error) bool {
	return err != nil &&
		   err != TopicExistsError &&
		   err != InvalidTypeError &&
		   err != NoCodecError &&
		   err != NoHandlerError &&
		   err != NoPartitionError &&
		   err != WrongTypeError &&
		   err != KeyNotExistsError &&
		   err != GeneralError &&
		   err != ConnectionDroppedError &&
		   err != FatalError
}