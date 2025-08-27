package codec

import (
	"testing"

	"github.com/fgrzl/streamkit/internal/txn"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTestEntry(seq uint64) *api.Entry {
	return &api.Entry{
		Sequence:  seq,
		Timestamp: 1234567890,
		TRX: api.TRX{
			ID:     uuid.New(),
			Node:   uuid.New(),
			Number: seq,
		},
		Payload:  []byte("hello world"),
		Metadata: map[string]string{"k": "v"},
		Space:    "space0",
		Segment:  "segment0",
	}
}

func TestEncodeDecodeEntryRoundtrip(t *testing.T) {
	t.Run("Given an entry", func(t *testing.T) {
		e := makeTestEntry(1)

		t.Run("When it is encoded and decoded", func(t *testing.T) {
			data, err := EncodeEntry(e)
			require.NoError(t, err)

			got := &api.Entry{}
			err = DecodeEntry(data, got)
			require.NoError(t, err)

			t.Run("Then the decoded entry should match the original", func(t *testing.T) {
				assert.Equal(t, e, got)
			})
		})
	})
}

func TestEncodeDecodeEntrySnappyRoundtrip(t *testing.T) {
	t.Run("Given an entry", func(t *testing.T) {
		e := makeTestEntry(2)

		t.Run("When it is encoded and decoded with snappy", func(t *testing.T) {
			data, err := EncodeEntrySnappy(e)
			require.NoError(t, err)

			got := &api.Entry{}
			err = DecodeEntrySnappy(data, got)
			require.NoError(t, err)

			t.Run("Then the decoded entry should match the original", func(t *testing.T) {
				assert.Equal(t, e, got)
			})
		})
	})
}

func makeTestTransaction() *txn.Transaction {
	e1 := makeTestEntry(1)
	e2 := makeTestEntry(2)
	trx := txn.Transaction{
		TRX:           e2.TRX,
		Space:         "space0",
		Segment:       "segment0",
		FirstSequence: e1.Sequence,
		LastSequence:  e2.Sequence,
		Entries:       []*api.Entry{e1, e2},
		Timestamp:     987654321,
	}
	return &trx
}

func TestEncodeDecodeTransactionRoundtrip(t *testing.T) {
	t.Run("Given a transaction", func(t *testing.T) {
		tr := makeTestTransaction()

		t.Run("When it is encoded and decoded", func(t *testing.T) {
			data, err := EncodeTransaction(tr)
			require.NoError(t, err)

			got := &txn.Transaction{}
			err = DecodeTransaction(data, got)
			require.NoError(t, err)

			t.Run("Then the decoded transaction should match the original", func(t *testing.T) {
				assert.Equal(t, tr, got)
			})
		})
	})
}

func TestEncodeDecodeTransactionSnappyRoundtrip(t *testing.T) {
	t.Run("Given a transaction", func(t *testing.T) {
		tr := makeTestTransaction()

		t.Run("When it is encoded and decoded with snappy", func(t *testing.T) {
			data, err := EncodeTransactionSnappy(tr)
			require.NoError(t, err)

			got := &txn.Transaction{}
			err = DecodeTransactionSnappy(data, got)
			require.NoError(t, err)

			t.Run("Then the decoded transaction should match the original", func(t *testing.T) {
				assert.Equal(t, tr, got)
			})
		})
	})
}
