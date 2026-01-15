package codec

import (
	"crypto/rand"
	"testing"

	"github.com/fgrzl/streamkit/internal/txn"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
)

func benchEntry(size int) *api.Entry {
	payload := make([]byte, size)
	_, _ = rand.Read(payload)
	return &api.Entry{
		Sequence:  42,
		Timestamp: 1730000000000,
		TRX: api.TRX{
			ID:     uuid.New(),
			Node:   uuid.New(),
			Number: 7,
		},
		Payload:  payload,
		Metadata: map[string]string{"k": "v"},
		Space:    "benchspace",
		Segment:  "seg-0",
	}
}

func benchTxn(entries, payloadSize int) *txn.Transaction {
	t := &txn.Transaction{
		TRX:           api.TRX{ID: uuid.New(), Node: uuid.New(), Number: 77},
		Space:         "benchspace",
		Segment:       "seg-0",
		FirstSequence: 1,
		LastSequence:  uint64(entries),
		Timestamp:     1730000000001,
		Entries:       make([]*api.Entry, 0, entries),
	}
	for i := 0; i < entries; i++ {
		e := benchEntry(payloadSize)
		e.Sequence = uint64(i + 1)
		t.Entries = append(t.Entries, e)
	}
	return t
}

func BenchmarkEncodeEntry_1KB(b *testing.B) {
	e := benchEntry(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeEntry(e); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeEntry_1KB(b *testing.B) {
	e := benchEntry(1024)
	buf, err := EncodeEntry(e)
	if err != nil {
		b.Fatal(err)
	}
	var out api.Entry
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = api.Entry{}
		if err := DecodeEntry(buf, &out); err != nil {
			b.Fatal(err)
		}
	}
	_ = out
}

func BenchmarkEncodeEntrySnappy_1KB(b *testing.B) {
	e := benchEntry(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeEntrySnappy(e); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeEntrySnappy_1KB(b *testing.B) {
	e := benchEntry(1024)
	buf, err := EncodeEntrySnappy(e)
	if err != nil {
		b.Fatal(err)
	}
	var out api.Entry
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = api.Entry{}
		if err := DecodeEntrySnappy(buf, &out); err != nil {
			b.Fatal(err)
		}
	}
	_ = out
}

func BenchmarkEncodeTransaction_10x1KB(b *testing.B) {
	t := benchTxn(10, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeTransaction(t); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeTransaction_10x1KB(b *testing.B) {
	t := benchTxn(10, 1024)
	buf, err := EncodeTransaction(t)
	if err != nil {
		b.Fatal(err)
	}
	var out txn.Transaction
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = txn.Transaction{}
		if err := DecodeTransaction(buf, &out); err != nil {
			b.Fatal(err)
		}
	}
	_ = out
}

func BenchmarkEncodeTransactionSnappy_10x1KB(b *testing.B) {
	t := benchTxn(10, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeTransactionSnappy(t); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeTransactionSnappy_10x1KB(b *testing.B) {
	t := benchTxn(10, 1024)
	buf, err := EncodeTransactionSnappy(t)
	if err != nil {
		b.Fatal(err)
	}
	var out txn.Transaction
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = txn.Transaction{}
		if err := DecodeTransactionSnappy(buf, &out); err != nil {
			b.Fatal(err)
		}
	}
	_ = out
}
