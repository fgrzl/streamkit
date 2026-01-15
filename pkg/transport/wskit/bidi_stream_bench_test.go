package wskit

import (
	"encoding/json"
	"testing"
)

// no-op encoder that counts bytes
type countingEncoder struct{ n int }

func (c *countingEncoder) send(p []byte) error { c.n += len(p); return nil }

func BenchmarkBidiStream_Encode(b *testing.B) {
	enc := &countingEncoder{}
	s := NewMuxerBidiStream(enc.send, nil)
	payload := map[string]any{"a": "b", "n": 123, "arr": []int{1, 2, 3}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Encode(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBidiStream_OfferDecode(b *testing.B) {
	enc := &countingEncoder{}
	s := NewMuxerBidiStream(enc.send, nil)

	// pre-marshal a typical payload
	msg := map[string]any{"x": "y", "v": 42}
	raw, _ := json.Marshal(msg)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Offer raw bytes
		if !s.Offer(raw) {
			b.Fatal("offer failed")
		}
		var out map[string]any
		if err := s.Decode(&out); err != nil {
			b.Fatal(err)
		}
	}
}
