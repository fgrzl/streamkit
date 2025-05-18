package txn

import (
	"encoding/json"
	"testing"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/timestamp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestTransactionMarshalJSON(t *testing.T) {
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

	trn := &Transaction{
		TRX:           trx,
		Space:         "space0",
		Segment:       "segment0",
		Entries:       entries,
		FirstSequence: 1,
		LastSequence:  1,
		Timestamp:     ts,
	}

	trnJSON, err := json.Marshal(trn)
	assert.NoError(t, err)
	assert.NotNil(t, trnJSON)

	trn2 := &Transaction{}
	err = json.Unmarshal(trnJSON, trn2)
	assert.NoError(t, err)
	assert.NotNil(t, trn2)

	assert.Equal(t, trn.TRX, trn2.TRX)
	assert.Equal(t, trn.Space, trn2.Space)
	assert.Equal(t, trn.Segment, trn2.Segment)
	assert.Equal(t, trn.FirstSequence, trn2.FirstSequence)
	assert.Equal(t, trn.LastSequence, trn2.LastSequence)
	assert.Equal(t, len(trn.Entries), len(trn2.Entries))
	assert.Equal(t, trn.Timestamp, trn2.Timestamp)
}
