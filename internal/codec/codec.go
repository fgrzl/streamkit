package codec

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/fgrzl/streamkit/internal/txn"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/golang/snappy"
)

// Constants for buffer initial sizes
const (
	defaultBufferSize = 1024
)

// EncodeEntrySnappy compresses a serialized Entry using Snappy
func EncodeEntrySnappy(e *api.Entry) ([]byte, error) {
	rawData, err := EncodeEntry(e) // First encode without compression
	if err != nil {
		return nil, err
	}

	// Compress the entry
	return snappy.Encode(nil, rawData), nil
}

// DecodeEntrySnappy decompresses and deserializes an Entry
func DecodeEntrySnappy(data []byte, e *api.Entry) error {
	// Decompress the entry first
	decompressedData, err := snappy.Decode(nil, data)
	if err != nil {
		return err
	}

	// Decode normally
	return DecodeEntry(decompressedData, e)
}

// EncodeEntry serializes an Entry into binary format
func EncodeEntry(e *api.Entry) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, defaultBufferSize))

	// Serialize fixed-size fields
	if err := binary.Write(buf, binary.LittleEndian, e.Sequence); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, e.Timestamp); err != nil {
		return nil, err
	}

	// Serialize TRX struct
	buf.Write(e.TRX.ID[:])
	buf.Write(e.TRX.Node[:])
	if err := binary.Write(buf, binary.LittleEndian, e.TRX.Number); err != nil {
		return nil, err
	}

	// Serialize variable-length fields
	if err := writeBytes(buf, e.Payload); err != nil {
		return nil, err
	}
	if err := writeMap(buf, e.Metadata); err != nil {
		return nil, err
	}
	if err := writeString(buf, e.Space); err != nil {
		return nil, err
	}
	if err := writeString(buf, e.Segment); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeEntry(data []byte, e *api.Entry) error {
	buf := bytes.NewReader(data)

	// Deserialize fixed-size fields
	if err := binary.Read(buf, binary.LittleEndian, &e.Sequence); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &e.Timestamp); err != nil {
		return err
	}

	// Deserialize TRX struct
	if _, err := buf.Read(e.TRX.ID[:]); err != nil {
		return err
	}
	if _, err := buf.Read(e.TRX.Node[:]); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &e.TRX.Number); err != nil {
		return err
	}

	// Deserialize variable-length fields
	payload, err := readBytes(buf)
	if err != nil {
		return err
	}
	e.Payload = payload

	metadata, err := readMap(buf)
	if err != nil {
		return err
	}
	e.Metadata = metadata

	space, err := readString(buf)
	if err != nil {
		return err
	}
	e.Space = space

	segment, err := readString(buf)
	if err != nil {
		return err
	}
	e.Segment = segment

	return nil
}

// EncodeEntrySnappy compresses a serialized Entry using Snappy
func EncodeTransactionSnappy(e *txn.Transaction) ([]byte, error) {
	rawData, err := EncodeTransaction(e) // First encode without compression
	if err != nil {
		return nil, err
	}

	// Compress the entry
	return snappy.Encode(nil, rawData), nil
}

// DecodeEntrySnappy decompresses and deserializes an Entry
func DecodeTransactionSnappy(data []byte, t *txn.Transaction) error {
	// Decompress the entry first
	decompressedData, err := snappy.Decode(nil, data)
	if err != nil {
		return err
	}

	// Decode normally
	return DecodeTransaction(decompressedData, t)
}

// EncodeTransaction serializes a Transaction into binary format
func EncodeTransaction(t *txn.Transaction) ([]byte, error) {
	estimatedSize := defaultBufferSize + len(t.Entries)*128
	buf := bytes.NewBuffer(make([]byte, 0, estimatedSize))

	// Serialize TRX
	buf.Write(t.TRX.ID[:])
	buf.Write(t.TRX.Node[:])
	if err := binary.Write(buf, binary.LittleEndian, t.TRX.Number); err != nil {
		return nil, err
	}

	// Serialize other fields
	if err := writeString(buf, t.Space); err != nil {
		return nil, err
	}
	if err := writeString(buf, t.Segment); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, t.FirstSequence); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, t.LastSequence); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, t.Timestamp); err != nil {
		return nil, err
	}

	// Serialize Entries
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(t.Entries))); err != nil {
		return nil, err
	}
	for _, entry := range t.Entries {
		entryData, err := EncodeEntry(entry)
		if err != nil {
			return nil, err
		}
		if err := writeBytes(buf, entryData); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// DecodeTransaction deserializes a Transaction from binary format
func DecodeTransaction(data []byte, t *txn.Transaction) error {
	buf := bytes.NewReader(data)

	// Deserialize TRX
	if _, err := buf.Read(t.TRX.ID[:]); err != nil {
		return err
	}
	if _, err := buf.Read(t.TRX.Node[:]); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &t.TRX.Number); err != nil {
		return err
	}

	// Deserialize other fields
	space, err := readString(buf)
	if err != nil {
		return err
	}
	t.Space = space

	segment, err := readString(buf)
	if err != nil {
		return err
	}
	t.Segment = segment

	if err := binary.Read(buf, binary.LittleEndian, &t.FirstSequence); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &t.LastSequence); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &t.Timestamp); err != nil {
		return err
	}

	// Deserialize Entries
	var entryCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &entryCount); err != nil {
		return err
	}

	t.Entries = make([]*api.Entry, entryCount)
	for i := range t.Entries {
		entryData, err := readBytes(buf)
		if err != nil {
			return err
		}

		entry := &api.Entry{}
		if DecodeEntry(entryData, entry) != nil {
			return err
		}

		t.Entries[i] = entry
	}

	return err
}

// Helper functions
func writeBytes(buf *bytes.Buffer, data []byte) error {
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := buf.Write(data)
	return err
}

func readBytes(buf *bytes.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	if length > uint32(buf.Len()) {
		return nil, errors.New("invalid data length")
	}
	data := make([]byte, length)
	_, err := buf.Read(data)
	return data, err
}

func writeString(buf *bytes.Buffer, s string) error {
	return writeBytes(buf, []byte(s))
}

func readString(buf *bytes.Reader) (string, error) {
	data, err := readBytes(buf)
	return string(data), err
}

func writeMap(buf *bytes.Buffer, m map[string]string) error {
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(m))); err != nil {
		return err
	}
	for k, v := range m {
		if err := writeString(buf, k); err != nil {
			return err
		}
		if err := writeString(buf, v); err != nil {
			return err
		}
	}
	return nil
}

func readMap(buf *bytes.Reader) (map[string]string, error) {
	var length uint32
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	m := make(map[string]string, length)
	for i := uint32(0); i < length; i++ {
		k, _ := readString(buf)
		v, _ := readString(buf)
		m[k] = v
	}
	return m, nil
}
