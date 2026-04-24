package wskit

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldEvictTombstonesOlderThanMaxAge(t *testing.T) {
	start := time.Unix(0, 0)
	now := start
	m := &WebSocketMuxer{
		Context:         context.Background(),
		name:            "server",
		done:            make(chan struct{}),
		channels:        make(map[uuid.UUID]*MuxerBidiStream),
		tombstones:      make(map[uuid.UUID]tombstoneEntry),
		tombstoneMax:    100,
		tombstoneMaxAge: 10 * time.Millisecond,
		nowFn:           func() time.Time { return now },
	}
	c1, c2, c3 := uuid.New(), uuid.New(), uuid.New()
	m.tombstoneStream(c1, ErrStreamOverloaded)
	m.tombstoneStream(c2, ErrStreamOverloaded)
	assert.Equal(t, 2, m.DiagnosticsSnapshot().TombstonesCount)

	now = start.Add(20 * time.Millisecond)
	m.tombstoneStream(c3, ErrStreamOverloaded)
	assert.Equal(t, 1, m.DiagnosticsSnapshot().TombstonesCount)
}

func TestShouldCapTombstonesBySizeLimit(t *testing.T) {
	fixed := time.Unix(1, 0)
	m := &WebSocketMuxer{
		Context:         context.Background(),
		name:            "server",
		done:            make(chan struct{}),
		channels:        make(map[uuid.UUID]*MuxerBidiStream),
		tombstones:      make(map[uuid.UUID]tombstoneEntry),
		tombstoneMax:    16,
		tombstoneMaxAge: 0,
		nowFn:           func() time.Time { return fixed },
	}
	for range 64 {
		m.tombstoneStream(uuid.New(), ErrTooManyStreams)
	}
	assert.LessOrEqual(t, m.DiagnosticsSnapshot().TombstonesCount, 16)
}

func TestShouldStillSilenceLateFramesWithinWindow(t *testing.T) {
	manager := &fakeNodeManager{}
	channelID := uuid.New()
	fixed := time.Unix(2, 0)
	m := &WebSocketMuxer{
		Context:         context.Background(),
		name:            "server",
		channels:        make(map[uuid.UUID]*MuxerBidiStream),
		tombstones:      make(map[uuid.UUID]tombstoneEntry),
		done:            make(chan struct{}),
		nodeManager:     manager,
		session:         &muxerSession{allowAll: true},
		tombstoneMax:    1024,
		tombstoneMaxAge: time.Minute,
		nowFn:           func() time.Time { return fixed },
	}
	m.tombstoneStream(channelID, ErrStreamOverloaded)

	_, err := m.getOrCreateStream(context.Background(), &MuxerMsg{
		ControlType: ControlTypeData,
		StoreID:     uuid.New(),
		ChannelID:   channelID,
		Payload:     []byte(`"late"`),
	})
	require.ErrorIs(t, err, ErrStreamTombstoned)
	assert.Equal(t, int32(0), manager.getCalls.Load())
}

func TestShouldTreatTombstoneAsUnknownAfterExpiry(t *testing.T) {
	manager := &fakeNodeManager{}
	channelID := uuid.New()
	start := time.Unix(10, 0)
	now := start
	m := &WebSocketMuxer{
		Context:         context.Background(),
		name:            "server",
		channels:        make(map[uuid.UUID]*MuxerBidiStream),
		tombstones:      make(map[uuid.UUID]tombstoneEntry),
		done:            make(chan struct{}),
		nodeManager:     manager,
		session:         &muxerSession{allowAll: true},
		tombstoneMax:    1024,
		tombstoneMaxAge: 5 * time.Millisecond,
		nowFn:           func() time.Time { return now },
	}
	m.tombstoneStream(channelID, ErrStreamOverloaded)
	now = start.Add(20 * time.Millisecond)

	_, err := m.getOrCreateStream(context.Background(), &MuxerMsg{
		ControlType: ControlTypeData,
		StoreID:     uuid.New(),
		ChannelID:   channelID,
		Payload:     []byte(`"fresh"`),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), manager.getCalls.Load())
}

// Regression: an expired tombstone must not block the same ChannelID from being registered again.
func TestShouldNotReaddChannelAfterItsTombstoneExpires(t *testing.T) {
	manager := &fakeNodeManager{}
	channelID := uuid.New()
	start := time.Unix(20, 0)
	now := start
	m := &WebSocketMuxer{
		Context:         context.Background(),
		name:            "server",
		channels:        make(map[uuid.UUID]*MuxerBidiStream),
		tombstones:      make(map[uuid.UUID]tombstoneEntry),
		done:            make(chan struct{}),
		nodeManager:     manager,
		session:         &muxerSession{allowAll: true},
		tombstoneMax:    1024,
		tombstoneMaxAge: 5 * time.Millisecond,
		nowFn:           func() time.Time { return now },
	}
	m.tombstoneStream(channelID, ErrStreamOverloaded)
	now = start.Add(20 * time.Millisecond)

	bidi, err := m.getOrCreateStream(context.Background(), &MuxerMsg{
		ControlType: ControlTypeData,
		StoreID:     uuid.New(),
		ChannelID:   channelID,
		Payload:     []byte(`"fresh"`),
	})
	require.NoError(t, err)
	require.NotNil(t, bidi)
	m.channelsMu.Lock()
	_, stillTomb := m.tombstones[channelID]
	m.channelsMu.Unlock()
	assert.False(t, stillTomb, "register path should delete tombstone for channel")
}
