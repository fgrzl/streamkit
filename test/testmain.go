package test

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// Disable jitter during tests to make integration tests deterministic and
	// avoid intermittent timing-related failures.
	os.Setenv("STREAMKIT_TEST_NO_JITTER", "1")
	os.Exit(m.Run())
}
