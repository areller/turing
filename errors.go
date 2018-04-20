package turing

import (
	"errors"
)

var (
	TopicExistsError = errors.New("Topic already exists")
	InvalidTypeError = errors.New("Object is of invalid type")
	NoCodecForTopic = errors.New("No codec is defined for the topic")
	NoPartition = errors.New("No such partition exists")
)