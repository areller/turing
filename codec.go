package turing

type DecodedKV struct {
	Key string
	Value interface{}
}

type EncodedKV struct {
	Key []byte
	Value []byte
}

type Codec interface {
	Decode(key []byte, value []byte) (DecodedKV, error)
	Encode(key string, msg interface{}) (EncodedKV, error)
}