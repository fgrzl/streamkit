package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/fgrzl/streamkit/internal/txn"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/klauspost/compress/zstd"
)

// Constants for buffer initial sizes
const (
	defaultBufferSize = 1024
)

// EncodeEntrySnappy compresses a serialized Entry using Zstd
func EncodeEntrySnappy(e *api.Entry) ([]byte, error) {
	rawData, err := EncodeEntry(e) // First encode without compression
	if err != nil {
		return nil, err
	}

	// Compress using zstd
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = encoder.Close() }()

	return encoder.EncodeAll(rawData, nil), nil
}

// DecodeEntrySnappy decompresses and deserializes an Entry
func DecodeEntrySnappy(data []byte, e *api.Entry) error {
	// Decompress the entry first
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer decoder.Close()

	decompressedData, err := decoder.DecodeAll(data, nil)
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

// EncodeTransactionSnappy compresses a serialized Transaction using Zstd
func EncodeTransactionSnappy(e *txn.Transaction) ([]byte, error) {
	rawData, err := EncodeTransaction(e) // First encode without compression
	if err != nil {
		return nil, err
	}

	// Compress using zstd
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = encoder.Close() }()

	return encoder.EncodeAll(rawData, nil), nil
}

// DecodeTransactionSnappy decompresses and deserializes a Transaction
func DecodeTransactionSnappy(data []byte, t *txn.Transaction) error {
	// Decompress the transaction first
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer decoder.Close()

	decompressedData, err := decoder.DecodeAll(data, nil)
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
	n := len(t.Entries)
	if n > math.MaxUint32 {
		return nil, errors.New("transaction has too many entries")
	}
	if err := writeUint32LE(buf, n); err != nil {
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
		if decErr := DecodeEntry(entryData, entry); decErr != nil {
			return decErr
		}

		t.Entries[i] = entry
	}

	return nil
}

// writeUint32LE appends n as a little-endian uint32 after validating n fits in 32 bits.
func writeUint32LE(buf *bytes.Buffer, n int) error {
	if n < 0 || n > math.MaxUint32 {
		return errors.New("length out of range for uint32")
	}
	v := uint32(n)
	return binary.Write(buf, binary.LittleEndian, v)
}

func writeBytes(buf *bytes.Buffer, data []byte) error {
	if len(data) > math.MaxUint32 {
		return errors.New("data too large")
	}
	if err := writeUint32LE(buf, len(data)); err != nil {
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
	if int64(length) > int64(buf.Len()) {
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
	if len(m) > math.MaxUint32 {
		return errors.New("map too large")
	}
	if err := writeUint32LE(buf, len(m)); err != nil {
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
		k, err := readString(buf)
		if err != nil {
			return nil, err
		}
		v, err := readString(buf)
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}
