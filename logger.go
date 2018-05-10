package turing

import (
	"io/ioutil"
	"github.com/sirupsen/logrus"
)

type LogFields = logrus.Fields

var Log *logrus.Entry = createDefaultLogger()

func createDefaultLogger() *logrus.Entry {
	logger := logrus.New()
	logger.Out = ioutil.Discard
	return logger.WithFields(LogFields{})
}

func SetLogger(logger *logrus.Entry) {
	Log = logger
}