package azurekit

import (
	"testing"

	"github.com/fgrzl/streamkit/internal/codec"
	"github.com/fgrzl/streamkit/internal/txn"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCreateTransactionEncodeDecode(t *testing.T) {
	entries := []*api.Entry{
		{Sequence: 1, Space: "space", Segment: "seg"},
		{Sequence: 2, Space: "space", Segment: "seg"},
	}
	trx := api.TRX{ID: uuid.New(), Number: 1}
	transaction := &txn.Transaction{
		TRX:           trx,
		Space:         "space",
		Segment:       "seg",
		FirstSequence: 1,
		LastSequence:  2,
		Entries:       entries,
	}

	ent, err := createTransactionEntity(transaction)
	require.NoError(t, err)

	decoded := &txn.Transaction{}
	require.NoError(t, codec.DecodeTransactionSnappy(ent.Value, decoded))

	require.Equal(t, transaction.Space, decoded.Space)
	require.Equal(t, transaction.Segment, decoded.Segment)
	require.Equal(t, transaction.FirstSequence, decoded.FirstSequence)
	require.Equal(t, transaction.LastSequence, decoded.LastSequence)
	require.Equal(t, len(transaction.Entries), len(decoded.Entries))
}
