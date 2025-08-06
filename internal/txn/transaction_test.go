package txn

import (
	"encoding/json"
	"testing"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/timestamp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestShouldMarshalAndUnmarshalTransactionWhenGivenValidData(t *testing.T) {
	// Arrange
	ts := timestamp.GetTimestamp()
	trx := api.TRX{Node: uuid.New(), ID: uuid.New(), Number: 1}
	entries := []*api.Entry{{
		TRX:       trx,
		Space:     "space0",
		Segment:   "segment0",
		Sequence:  1,
		Timestamp: ts,
		Payload:   []byte("data"),
	}}

	originalTransaction := &Transaction{
		TRX:           trx,
		Space:         "space0",
		Segment:       "segment0",
		Entries:       entries,
		FirstSequence: 1,
		LastSequence:  1,
		Timestamp:     ts,
	}

	// Act
	trnJSON, err := json.Marshal(originalTransaction)
	assert.NoError(t, err)
	assert.NotNil(t, trnJSON)

	unmarshaledTransaction := &Transaction{}
	err = json.Unmarshal(trnJSON, unmarshaledTransaction)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, unmarshaledTransaction)
	assert.Equal(t, originalTransaction.TRX, unmarshaledTransaction.TRX)
	assert.Equal(t, originalTransaction.Space, unmarshaledTransaction.Space)
	assert.Equal(t, originalTransaction.Segment, unmarshaledTransaction.Segment)
	assert.Equal(t, originalTransaction.FirstSequence, unmarshaledTransaction.FirstSequence)
	assert.Equal(t, originalTransaction.LastSequence, unmarshaledTransaction.LastSequence)
	assert.Equal(t, len(originalTransaction.Entries), len(unmarshaledTransaction.Entries))
	assert.Equal(t, originalTransaction.Timestamp, unmarshaledTransaction.Timestamp)
}
