package test

import (
	"testing"
	"time"

	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var testWorkerA = uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")

func inventoryContains(inventory *client.WorkerInventory, workerID uuid.UUID) bool {
	if inventory == nil {
		return false
	}
	for _, worker := range inventory.Workers {
		if worker.WorkerID == workerID {
			return true
		}
	}
	return false
}

func TestShouldDeliverInventoryGivenSubscribeWithWorkerID(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			if name == "azure" {
				t.Skip("azure harness requires local Fazure")
			}

			harness := h(t)
			ctx := t.Context()
			storeID := uuid.New()

			updates := make(chan client.WorkerInventory, 16)

			observer, err := harness.SubscribeWorkers(ctx, storeID, func(inventory *client.WorkerInventory) {
				if inventory == nil {
					return
				}
				select {
				case updates <- *inventory:
				default:
				}
			})
			require.NoError(t, err)
			t.Cleanup(func() { observer.Unsubscribe() })

			worker, err := harness.SubscribeWorkers(ctx, storeID, func(*client.WorkerInventory) {},
				client.WithWorkerID(testWorkerA),
				client.WithWorkerMetadata(map[string]string{"role": "processor"}),
			)
			require.NoError(t, err)
			t.Cleanup(func() { worker.Unsubscribe() })

			require.Eventually(t, func() bool {
				select {
				case update := <-updates:
					return inventoryContains(&update, testWorkerA)
				default:
					return false
				}
			}, 5*time.Second, 25*time.Millisecond)

			worker.Unsubscribe()

			require.Eventually(t, func() bool {
				select {
				case update := <-updates:
					return !inventoryContains(&update, testWorkerA)
				default:
					return false
				}
			}, 5*time.Second, 25*time.Millisecond)
		})
	}
}

func TestShouldIsolateInventoryByStoreID(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			if name == "azure" {
				t.Skip("azure harness requires local Fazure")
			}

			harness := h(t)
			ctx := t.Context()
			storeA := uuid.New()
			storeB := uuid.New()

			updates := make(chan client.WorkerInventory, 8)
			sub, err := harness.SubscribeWorkers(ctx, storeB, func(inventory *client.WorkerInventory) {
				if inventory == nil {
					return
				}
				select {
				case updates <- *inventory:
				default:
				}
			})
			require.NoError(t, err)
			t.Cleanup(func() { sub.Unsubscribe() })

			worker, err := harness.SubscribeWorkers(ctx, storeA, func(*client.WorkerInventory) {},
				client.WithWorkerID(testWorkerA),
			)
			require.NoError(t, err)
			t.Cleanup(func() { worker.Unsubscribe() })

			time.Sleep(300 * time.Millisecond)
			select {
			case update := <-updates:
				if inventoryContains(&update, testWorkerA) {
					t.Fatalf("store B subscriber should not receive store A worker, got %#v", update)
				}
			default:
			}
		})
	}
}
