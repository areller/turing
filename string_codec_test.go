package turing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncoding(t *testing.T) {
	sc := new(StringCodec)
	encoded, err := sc.Encode("myKey", "My Message")

	assert.Equal(t, nil, err)
	assert.Equal(t, []byte("myKey"), encoded.Key)
	assert.Equal(t, []byte("My Message"), encoded.Value)
}

func TestDecoding(t *testing.T) {
	sc := new(StringCodec)
	decoded, err := sc.Decode([]byte("myKey"), []byte("My Message"))

	assert.Equal(t, nil, err)
	assert.Equal(t, "myKey", decoded.Key)
	assert.Equal(t, "My Message", decoded.Value)
}