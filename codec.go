package mqwrap

/*
   Encoding and Decoding interfaces for sending and receiving formatted messages over rabbit.

   To create your own, implement the Compresser and Encoder interfaces and register them
   using the RegisterEncoding/RegisterCompression functions with the first argument as the
   string to look for in the headers to apply that Encoding/Compression

*/

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"encoding/xml"
	"errors"

	"github.com/zeebo/bencode"
	"golang.org/x/net/html/charset"
)

type Compresser interface {
	To([]byte) ([]byte, error)
	From([]byte) ([]byte, error)
}

type Encoder interface {
	To(interface{}) ([]byte, error)
	From([]byte, interface{}) (interface{}, error)
}

var Compressers = make(map[string]Compresser)
var Encoders = make(map[string]Encoder)

func init() {
	//Register the basic encodings
	RegisterEncoding("application/json", JSONEncoding{})
	RegisterEncoding("application/xml", XMLEncoding{})
	RegisterEncoding("application/bencode", BEncoding{})

	RegisterCompression("gzip", GZIPCompression{})
	RegisterCompression("deflate", DeflateCompression{})
	RegisterCompression("UTF-8", NoCompression{})
}

func RegisterCompression(name string, c Compresser) {
	Compressers[name] = c
}

func RegisterEncoding(name string, e Encoder) {
	Encoders[name] = e
}

func Encode(m interface{}, contentType string) ([]byte, error) {
	if enc, ok := Encoders[contentType]; ok {
		return enc.To(m)
	}
	return nil, errors.New("Encoding not defined: " + contentType)
}

func Decode(b []byte, contentType string, msg interface{}) (interface{}, error) {
	if enc, ok := Encoders[contentType]; ok {
		return enc.From(b, msg)
	}
	return nil, errors.New("Encoding not defined: " + contentType)
}

func Compress(b []byte, contentEncoding string) ([]byte, error) {

	if contentEncoding == "" {
		return b, nil
	}

	if comp, ok := Compressers[contentEncoding]; ok {
		return comp.To(b)
	}

	return nil, errors.New("Compression not defined: " + contentEncoding)
}

func Decompress(b []byte, contentEncoding string) ([]byte, error) {

	if contentEncoding == "" {
		return b, nil
	}

	if comp, ok := Compressers[contentEncoding]; ok {
		return comp.From(b)
	}
	return nil, errors.New("Compression not defined: " + contentEncoding)
}

type DeflateCompression struct{}

//TODO:: optimization opportunity
func (d DeflateCompression) To(uncompressed []byte) ([]byte, error) {
	compressed := bytes.NewBuffer([]byte{})
	w := zlib.NewWriter(compressed)
	_, err := w.Write(uncompressed)
	w.Close()
	return compressed.Bytes(), err
}

func (d DeflateCompression) From(compressed []byte) ([]byte, error) {
	uncompressed := bytes.NewBuffer([]byte{})
	r, err := zlib.NewReader(bytes.NewBuffer(compressed))
	if err != nil {
		return uncompressed.Bytes(), err
	}
	_, err = uncompressed.ReadFrom(r)
	return uncompressed.Bytes(), err
}

type GZIPCompression struct{}

//TODO:: optimization opportunity
func (g GZIPCompression) To(uncompressed []byte) ([]byte, error) {
	compressed := bytes.NewBuffer([]byte{})
	w := gzip.NewWriter(compressed)
	_, err := w.Write(uncompressed)
	w.Close()
	return compressed.Bytes(), err
}

func (g GZIPCompression) From(compressed []byte) ([]byte, error) {
	uncompressed := bytes.NewBuffer([]byte{})
	r, err := gzip.NewReader(bytes.NewBuffer(compressed))
	if err != nil {
		return uncompressed.Bytes(), err
	}
	_, err = uncompressed.ReadFrom(r)
	return uncompressed.Bytes(), err
}

type NoCompression struct{}

//TODO:: optimization opportunity
func (n NoCompression) To(uncompressed []byte) ([]byte, error) {
	return uncompressed, nil
}

func (n NoCompression) From(compressed []byte) ([]byte, error) {
	return compressed, nil
}

type JSONEncoding struct{}

func (j JSONEncoding) To(m interface{}) ([]byte, error) {
	return json.Marshal(m)
}

func (j JSONEncoding) From(b []byte, msg interface{}) (interface{}, error) {
	err := json.Unmarshal(b, &msg)
	return msg, err
}

type BEncoding struct{}

func (b BEncoding) To(m interface{}) ([]byte, error) {
	return bencode.EncodeBytes(m)
}

func (be BEncoding) From(b []byte, msg interface{}) (interface{}, error) {
	err := bencode.DecodeBytes(b, &msg)
	return msg, err
}

type XMLEncoding struct{}

func (x XMLEncoding) To(m interface{}) ([]byte, error) {
	return xml.Marshal(m)
}

func (x XMLEncoding) From(b []byte, msg interface{}) (interface{}, error) {
	var err error
	decoder := xml.NewDecoder(bytes.NewBuffer(b))
	decoder.CharsetReader = charset.NewReaderLabel
	err = decoder.Decode(&msg)
	return msg, err
}
