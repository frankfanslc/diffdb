package diffdb

import (
	"bytes"
	"gopkg.in/vmihailenco/msgpack.v2"
)

// A Decoder decodes serialised byte data of a diff entry into a native object.
// The object passed to Decode should be the same type added to the diff.
type Decoder interface {
	Decode(interface{}) error
}

var _ Decoder = (*msgpackDecoder)(nil)

// msgpackDecoder uses the msgpack library to unmarshal differential data
type msgpackDecoder struct {
	data []byte
}

func (msg *msgpackDecoder) Decode(x interface{}) error {
	r := bytes.NewReader(msg.data)
	return msgpack.NewDecoder(r).Decode(x)
}
