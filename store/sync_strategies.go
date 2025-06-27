package store

import (
	"context"
	"fmt"
	"time"
)

type Syncable interface {
	Sync()
}

type StoreSyncStrategy interface {
	OnStoreOpened(syncable Syncable)
	OnDataCopyFinished(syncable Syncable)
	OnCloseStore(syncable Syncable)
}

type NoSyncStrategy struct{}

func (nss *NoSyncStrategy) OnStoreOpened(syncable Syncable)      { return }
func (nss *NoSyncStrategy) OnDataCopyFinished(syncable Syncable) { return }
func (nss *NoSyncStrategy) OnCloseStore(syncable Syncable)       { return }

type PeriodicSyncStrategy struct {
	delay  time.Duration
	ctx    context.Context
	cancel context.CancelFunc
}

func NewPeriodicSyncStrategy(delay time.Duration) *PeriodicSyncStrategy {
	ctx, cancel := context.WithCancel(context.Background())
	return &PeriodicSyncStrategy{delay, ctx, cancel}
}

func (pss *PeriodicSyncStrategy) OnStoreOpened(syncable Syncable) {
	go pss.syncDataPeriodic(syncable)
}

func (pss *PeriodicSyncStrategy) OnDataCopyFinished(syncable Syncable) { return }

func (pss *PeriodicSyncStrategy) OnCloseStore(syncable Syncable) {
	pss.cancel()
}

func (pss *PeriodicSyncStrategy) syncDataPeriodic(syncable Syncable) {
	fmt.Printf("Starting periodic data sync [%v interval]\n", pss.delay)
	ticker := time.NewTicker(pss.delay)
	defer func() {
		fmt.Printf("Stopping periodic data sync [%v interval]\n", pss.delay)
		ticker.Stop()
	}()

	for range ticker.C {
		select {
		case <-pss.ctx.Done():
			fmt.Printf("Periodic data sync received cancel signal (performing final sync)\n")
			syncable.Sync()
			return
		default:
			fmt.Printf("[msync]\n")
			syncable.Sync()
		}
	}
}
