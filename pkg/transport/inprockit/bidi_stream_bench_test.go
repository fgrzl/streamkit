package inprockit

import (
	"bytes"
	"encoding/json"
	"testing"
)

// BenchmarkLoopbackEncodeDecode measures throughput of a single stream where
// Encode writes are delivered back to Decode on the same stream.
func BenchmarkLoopbackEncodeDecode(b *testing.B) {
	type msg struct{ Text string }
	for i := 0; i < b.N; i++ {
		s := NewInProcBidiStreamLoopback()
		b.StopTimer()
		// prepare once
		b.StartTimer()
		if err := s.Encode(msg{Text: "hello"}); err != nil {
			b.Fatalf("encode failed: %v", err)
		}
		var out msg
		if err := s.Decode(&out); err != nil {
			b.Fatalf("decode failed: %v", err)
		}
		s.Close(nil)
	}
}

// BenchmarkLinkedStreams measures throughput when two streams are linked via LinkStreams.
func BenchmarkLinkedStreams(b *testing.B) {
	type msg struct{ Text string }
	for i := 0; i < b.N; i++ {
		client := NewInProcBidiStream()
		server := NewInProcBidiStream()
		LinkStreams(client, server)

		b.StopTimer()
		b.StartTimer()

		if err := client.Encode(msg{Text: "hello"}); err != nil {
			b.Fatalf("client encode failed: %v", err)
		}
		var out msg
		if err := server.Decode(&out); err != nil {
			b.Fatalf("server decode failed: %v", err)
		}
	}
}

// BenchmarkParallelEncodes runs concurrent encoders against a linked pair to
// measure contention and channel overhead.
func BenchmarkParallelEncodes(b *testing.B) {
	type msg struct{ N int }
	client := NewInProcBidiStream()
	server := NewInProcBidiStream()
	LinkStreams(client, server)
	defer client.Close(nil)
	defer server.Close(nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Encode(msg{N: 1}); err != nil {
				b.Fatalf("encode failed: %v", err)
			}
			var out msg
			if err := server.Decode(&out); err != nil {
				b.Fatalf("decode failed: %v", err)
			}
		}
	})

	client.Close(nil)
	server.Close(nil)
}

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
