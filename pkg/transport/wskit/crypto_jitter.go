package wskit

import (
	crand "crypto/rand"
	"encoding/binary"
	"io"
	"math"
	"math/bits"
)

// int64nCrypto returns a uniform value in [0, n) using crypto/rand. If n <= 0
// or reading random bytes fails, it returns 0.
func int64nCrypto(n int64) int64 {
	if n <= 0 {
		return 0
	}
	var buf [8]byte
	if _, err := io.ReadFull(crand.Reader, buf[:]); err != nil {
		return 0
	}
	u := binary.LittleEndian.Uint64(buf[:])
	_, rem := bits.Div64(0, u, uint64(n))
	if rem > uint64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(rem)
}

// intnCrypto returns a uniform value in [0, n) using crypto/rand.
func intnCrypto(n int) int {
	if n <= 0 {
		return 0
	}
	return int(int64nCrypto(int64(n)))
}
