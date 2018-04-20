package turing

type StringKeyValue struct {
	Key string
	Value string
}

type StringCodec struct {

}

func (sc *StringCodec) Decode(key []byte, value []byte) (DecodedKV, error) {
	return DecodedKV{
		Key: string(key),
		Value: string(value),
	}, nil
}

func (sc *StringCodec) Encode(key string, msg interface{}) (EncodedKV, error) {
	str, ok := msg.(string)
	if !ok {
		return EncodedKV{}, InvalidTypeError
	}

	return EncodedKV{
		Key: []byte(key),
		Value: []byte(str),
	}, nil
}