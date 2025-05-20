package pebble

import (
	"context"
	"errors"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
)

var _ enumerators.Enumerator[KeyValuePair] = (*PebbleEnumerator)(nil)

// PebbleEnumerator wraps a Pebble iterator and implements Enumerator.
type PebbleEnumerator struct {
	opts     *pebble.IterOptions
	iter     *pebble.Iterator
	seek     func(*pebble.Iterator, *pebble.IterOptions) bool
	next     func(*pebble.Iterator) bool
	seeked   bool
	valid    bool
	err      error
	disposed sync.Once
}

// NewPebbleEnumerator creates a new enumerator for a given Pebble iterator.
func NewPebbleEnumerator(ctx context.Context, db *pebble.DB, opts *pebble.IterOptions) enumerators.Enumerator[KeyValuePair] {

	iter, err := db.NewIterWithContext(ctx, opts)
	if err != nil {
		return enumerators.Error[KeyValuePair](err)
	}

	return &PebbleEnumerator{
		iter: iter,
		opts: opts,
		seek: func(iter *pebble.Iterator, opts *pebble.IterOptions) bool { return iter.SeekGE(opts.LowerBound) },
		next: func(iter *pebble.Iterator) bool { return iter.Next() },
	}
}

// MoveNext advances the iterator and returns whether there is a next element.
func (e *PebbleEnumerator) MoveNext() bool {
	if e.iter == nil {
		return false
	}

	if !e.seeked {
		e.valid = e.seek(e.iter, e.opts)
		if !e.valid {
			e.err = e.iter.Error()
		}
		e.seeked = true
		return e.valid
	}

	e.valid = e.next(e.iter)
	if !e.valid {
		e.err = e.iter.Error()
	}
	return e.valid
}

// Current returns the current key-value pair or an error if iteration is invalid.
func (e *PebbleEnumerator) Current() (KeyValuePair, error) {

	if !e.valid {
		return KeyValuePair{}, errors.New("iterator is not valid")
	}
	// copy the key and value to prevent them from being overwritten
	key := append([]byte(nil), e.iter.Key()...)
	value := append([]byte(nil), e.iter.Value()...)

	return KeyValuePair{
		Key:   key,
		Value: value,
	}, nil

}

// Err returns any encountered error.
func (e *PebbleEnumerator) Err() error {
	return e.err
}

// Dispose closes the iterator.
func (e *PebbleEnumerator) Dispose() {
	e.disposed.Do(func() {
		e.iter.Close()
		e.iter = nil
	})
}

type KeyValuePair struct {
	Key   lexkey.LexKey
	Value []byte
}
