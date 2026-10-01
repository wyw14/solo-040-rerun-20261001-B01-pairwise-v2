package admission

import (
	"errors"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

type QueueEntry struct {
	Vessel   model.Vessel `json:"vessel"`
	PlanID   uuid.UUID    `json:"plan_id"`
	Released bool         `json:"released"`
	seq      uint64
}

// Queue preserves FIFO order while supporting convoy cancellation recovery.
type Queue struct {
	mu      sync.RWMutex
	entries map[string][]QueueEntry
	counter uint64
}

func NewQueue() *Queue { return &Queue{entries: make(map[string][]QueueEntry)} }

func (q *Queue) Add(chamberID string, planID uuid.UUID, vessels []model.Vessel) error {
	if chamberID == "" || planID == uuid.Nil || len(vessels) == 0 {
		return errors.New("queue addition requires chamber, plan and vessels")
	}
	q.mu.Lock()
	for _, vessel := range vessels {
		q.counter++
		q.entries[chamberID] = append(q.entries[chamberID], QueueEntry{
			Vessel: vessel, PlanID: planID, seq: q.counter,
		})
	}
	q.mu.Unlock()
	return nil
}

func (q *Queue) Peek(chamberID string) (QueueEntry, bool) {
	q.mu.RLock()
	entries := q.entries[chamberID]
	q.mu.RUnlock()
	if len(entries) == 0 {
		return QueueEntry{}, false
	}
	return entries[0], true
}

func (q *Queue) MarkReleased(chamberID string, vesselID uuid.UUID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for index := range q.entries[chamberID] {
		if q.entries[chamberID][index].Vessel.ID == vesselID {
			q.entries[chamberID][index].Released = true
			return true
		}
	}
	return false
}

// RemovePlan detaches every entry of a plan while preserving their original
// ordering and release flags. The returned slice is owned by the caller and is
// safe to hand back to Restore.
func (q *Queue) RemovePlan(chamberID string, planID uuid.UUID) []QueueEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	entries := q.entries[chamberID]
	kept := entries[:0]
	var removed []QueueEntry
	for _, entry := range entries {
		if entry.PlanID == planID {
			removed = append(removed, entry)
		} else {
			kept = append(kept, entry)
		}
	}
	q.entries[chamberID] = append([]QueueEntry(nil), kept...)
	return removed
}

// Restore is the exact inverse of RemovePlan: entries are returned to their
// original FIFO positions with their release flags intact. Entries that are
// already present (for example after a concurrent operation) are skipped, so a
// compensation rollback never duplicates a vessel or reorders the queue.
func (q *Queue) Restore(chamberID string, entries []QueueEntry) {
	if len(entries) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	present := make(map[uint64]bool, len(q.entries[chamberID])+len(entries))
	for _, entry := range q.entries[chamberID] {
		present[entry.seq] = true
	}
	merged := append([]QueueEntry(nil), q.entries[chamberID]...)
	for _, entry := range entries {
		if !present[entry.seq] {
			merged = append(merged, entry)
			present[entry.seq] = true
		}
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].seq < merged[j].seq })
	q.entries[chamberID] = merged
}

func (q *Queue) List(chamberID string) []QueueEntry {
	q.mu.RLock()
	result := append([]QueueEntry(nil), q.entries[chamberID]...)
	q.mu.RUnlock()
	return result
}
