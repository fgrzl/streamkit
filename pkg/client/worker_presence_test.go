package client

import (
	"testing"
	"time"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestWithWorkerHeartbeatIntervalClampsToMax(t *testing.T) {
	cfg := subscribeWorkersConfig{}
	WithWorkerHeartbeatInterval(2 * time.Hour)(&cfg)
	require.EqualValues(t, api.MaxWorkerPresenceHeartbeatIntervalSeconds, cfg.heartbeatIntervalSeconds)
}

func TestSubscribeWorkersUsesClampedRenewIntervalOnWire(t *testing.T) {
	cfg := subscribeWorkersConfig{workerID: mustTestWorkerID()}
	WithWorkerHeartbeatInterval(2 * time.Hour)(&cfg)

	seconds := api.WorkerPresenceRenewIntervalSeconds(cfg.heartbeatIntervalSeconds)
	require.EqualValues(t, api.MaxWorkerPresenceHeartbeatIntervalSeconds, seconds)
}

func mustTestWorkerID() uuid.UUID {
	return uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
}
