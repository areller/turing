package turing

type Decoder interface {
	DecodeKey(key []byte) string
	DecodeValue(value []byte) interface{}
}