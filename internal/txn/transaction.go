package txn

import "github.com/fgrzl/streamkit/pkg/api"

type Transaction struct {
	TRX           api.TRX      `json:"trx"`
	Space         string       `json:"space"`
	Segment       string       `json:"segment"`
	FirstSequence uint64       `json:"first_sequence"`
	LastSequence  uint64       `json:"last_sequence"`
	Entries       []*api.Entry `json:"entries"`
	Timestamp     int64        `json:"timestamp"`
}
