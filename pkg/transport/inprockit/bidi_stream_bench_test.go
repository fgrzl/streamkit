package inprockit

import (
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

		client.Close(nil)
		server.Close(nil)
	}
}

// BenchmarkParallelEncodes runs concurrent encoders against a linked pair to
// measure contention and channel overhead.
func BenchmarkParallelEncodes(b *testing.B) {
	type msg struct{ N int }
	client := NewInProcBidiStream()
	server := NewInProcBidiStream()
	LinkStreams(client, server)

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
