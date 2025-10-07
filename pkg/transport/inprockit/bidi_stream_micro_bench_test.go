package inprockit

import (
	"bytes"
	"encoding/json"
	"testing"
)

type benchMsg struct {
	Text string `json:"text"`
}

func BenchmarkJSONMarshal(b *testing.B) {
	m := benchMsg{Text: "hello world"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(m)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONEncoderPooled(b *testing.B) {
	m := benchMsg{Text: "hello world"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := payloadBufPool.Get().(*bytes.Buffer)
		buf.Reset()
		enc := json.NewEncoder(buf)
		if err := enc.Encode(m); err != nil {
			b.Fatal(err)
		}
		payloadBufPool.Put(buf)
	}
}

func BenchmarkJSONUnmarshalFromPooled(b *testing.B) {
	m := benchMsg{Text: "hello world"}
	// prepare payload
	buf := payloadBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	_ = json.NewEncoder(buf).Encode(m)
	payload := append([]byte(nil), buf.Bytes()...)
	payloadBufPool.Put(buf)

	var out benchMsg
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out = benchMsg{}
		if err := json.Unmarshal(payload, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStreamEncodeDecode(b *testing.B) {
	m := benchMsg{Text: "hello world"}
	var out benchMsg
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// encode to pooled buffer
		buf := payloadBufPool.Get().(*bytes.Buffer)
		buf.Reset()
		if err := json.NewEncoder(buf).Encode(m); err != nil {
			b.Fatal(err)
		}
		// decode from buffer
		if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
			b.Fatal(err)
		}
		payloadBufPool.Put(buf)
	}
}
