package api

import (
	"bytes"
	"sort"
	"time"

	"github.com/fgrzl/streamkit/pkg/bus"
	"github.com/google/uuid"
)

const (
	// DefaultWorkerPresenceRenewIntervalSeconds is used when a worker does not
	// specify HeartbeatIntervalSeconds on SubscribeWorkers.
	DefaultWorkerPresenceRenewIntervalSeconds = 30
	// MinWorkerPresenceHeartbeatIntervalSeconds is the minimum renewal/heartbeat period.
	MinWorkerPresenceHeartbeatIntervalSeconds = 1
	// MaxWorkerPresenceHeartbeatIntervalSeconds is the maximum renewal/heartbeat period.
	MaxWorkerPresenceHeartbeatIntervalSeconds = 300
	workerPresenceTTLGraceMultiplier          = 3
	minWorkerPresenceTTLSeconds               = 30
)

// SubscribeWorkers opens a long-lived bidirectional stream from a client to the
// store's single server node. Observer clients receive the full worker inventory
// on connect and whenever it changes. When WorkerID is set, this stream also
// registers that client as a worker in the node-local map until closed.
//
// For workers, HeartbeatIntervalSeconds controls three coupled behaviors: the
// client presence-renewal period, server→client stream heartbeats, and the
// stale-worker TTL (3× interval, minimum 30s). Observers should leave it at zero.
type SubscribeWorkers struct {
	WorkerID                 uuid.UUID         `json:"worker_id,omitempty"`
	Metadata                 map[string]string `json:"metadata,omitempty"`
	HeartbeatIntervalSeconds int64             `json:"heartbeat_interval_seconds"`
}

func (m *SubscribeWorkers) GetDiscriminator() string {
	return "streamkit://api/v1/subscribe_workers"
}

// WorkerInfo describes one worker in a store inventory.
type WorkerInfo struct {
	WorkerID   uuid.UUID         `json:"worker_id"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	LastSeenAt int64             `json:"last_seen_at"`
}

// WorkerInventory is the full set of online workers for a store.
type WorkerInventory struct {
	Workers   []WorkerInfo `json:"workers"`
	Timestamp int64        `json:"timestamp"`
}

func (m *WorkerInventory) GetDiscriminator() string {
	return "streamkit://api/v1/worker_inventory"
}

// Presence is sent on a worker's bidi stream. Server→client frames set Heartbeat
// true (not fanned out). Client→server renewals set Heartbeat false and refresh
// the worker's last_seen_at without notifying observers.
type Presence struct {
	WorkerID  uuid.UUID `json:"worker_id,omitempty"`
	Heartbeat bool      `json:"heartbeat"`
	Timestamp int64     `json:"timestamp"`
}

func (m *Presence) GetDiscriminator() string {
	return "streamkit://api/v1/presence"
}

// WorkerInventoryNotification is a reserved wire type. Worker presence is
// authoritative in memory on one server node per store; many clients connect via
// SubscribeWorkers. Inventory is not replicated across nodes or the message bus.
type WorkerInventoryNotification struct {
	StoreID   uuid.UUID        `json:"store_id"`
	Inventory *WorkerInventory `json:"inventory"`
}

func (obj *WorkerInventoryNotification) GetDiscriminator() string {
	return "streamkit://api/v1/worker_inventory_notification"
}

func (obj *WorkerInventoryNotification) GetRoute() bus.Route {
	if obj == nil {
		return bus.Route{}
	}
	return GetWorkerPresenceRoute(obj.StoreID)
}

func GetWorkerPresenceRoute(storeID uuid.UUID) bus.Route {
	return bus.NewTenantRoute("streamkit", "worker_presence", &storeID)
}

func NewWorkerInventoryNotification(storeID uuid.UUID, inventory *WorkerInventory) *WorkerInventoryNotification {
	return &WorkerInventoryNotification{
		StoreID:   storeID,
		Inventory: CloneWorkerInventory(inventory),
	}
}

// CloneWorkerInventory returns a snapshot safe to hand to another goroutine.
// Metadata maps are shared by reference; callers must treat the result as read-only.
func CloneWorkerInventory(inventory *WorkerInventory) *WorkerInventory {
	if inventory == nil {
		return nil
	}
	cloned := &WorkerInventory{
		Timestamp: inventory.Timestamp,
		Workers:   make([]WorkerInfo, len(inventory.Workers)),
	}
	copy(cloned.Workers, inventory.Workers)
	return cloned
}

func SortWorkerInventory(inventory *WorkerInventory) {
	if inventory == nil {
		return
	}
	sort.Slice(inventory.Workers, func(i, j int) bool {
		return CompareWorkerID(inventory.Workers[i].WorkerID, inventory.Workers[j].WorkerID) < 0
	})
}

func CompareWorkerID(a, b uuid.UUID) int {
	return bytes.Compare(a[:], b[:])
}

func NowUnixMilli() int64 {
	return time.Now().UnixMilli()
}

// ClampWorkerPresenceHeartbeatIntervalSeconds bounds a positive heartbeat interval
// to [MinWorkerPresenceHeartbeatIntervalSeconds, MaxWorkerPresenceHeartbeatIntervalSeconds].
// Non-positive values return 0 (observer / use default renew interval).
func ClampWorkerPresenceHeartbeatIntervalSeconds(seconds int64) int64 {
	if seconds <= 0 {
		return 0
	}
	if seconds < MinWorkerPresenceHeartbeatIntervalSeconds {
		return MinWorkerPresenceHeartbeatIntervalSeconds
	}
	if seconds > MaxWorkerPresenceHeartbeatIntervalSeconds {
		return MaxWorkerPresenceHeartbeatIntervalSeconds
	}
	return seconds
}

// WorkerPresenceRenewIntervalSeconds returns the client renewal period for a
// worker subscription. Non-positive values use DefaultWorkerPresenceRenewIntervalSeconds.
func WorkerPresenceRenewIntervalSeconds(heartbeatIntervalSeconds int64) int64 {
	clamped := ClampWorkerPresenceHeartbeatIntervalSeconds(heartbeatIntervalSeconds)
	if clamped > 0 {
		return clamped
	}
	return DefaultWorkerPresenceRenewIntervalSeconds
}

// WorkerPresenceTTL returns how long a worker may go without a successful
// client renewal before the server evicts it.
func WorkerPresenceTTL(renewIntervalSeconds int64) time.Duration {
	seconds := WorkerPresenceRenewIntervalSeconds(renewIntervalSeconds) * workerPresenceTTLGraceMultiplier
	if seconds < minWorkerPresenceTTLSeconds {
		seconds = minWorkerPresenceTTLSeconds
	}
	return time.Duration(seconds) * time.Second
}
