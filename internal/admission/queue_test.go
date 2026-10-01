package admission

import (
	"testing"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func testVessels(names ...string) []model.Vessel {
	vessels := make([]model.Vessel, 0, len(names))
	for _, name := range names {
		vessels = append(vessels, model.Vessel{ID: uuid.New(), Name: name, CallSign: name, LengthM: 10})
	}
	return vessels
}

func vesselIDs(entries []QueueEntry) []string {
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.Vessel.Name)
	}
	return ids
}

func TestRemoveAndRestoreKeepsFIFOAndReleaseFlags(t *testing.T) {
	queue := NewQueue()
	planA, planB := uuid.New(), uuid.New()
	vesselsA := testVessels("a1", "a2")
	vesselsB := testVessels("b1")

	if err := queue.Add("chamber-1", planA, vesselsA); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add("chamber-1", planB, vesselsB); err != nil {
		t.Fatal(err)
	}
	queue.MarkReleased("chamber-1", vesselsA[0].ID)

	removed := queue.RemovePlan("chamber-1", planA)
	if got := vesselIDs(queue.List("chamber-1")); len(got) != 1 || got[0] != "b1" {
		t.Fatalf("after remove head should be b1, got %v", got)
	}
	if !removed[0].Released {
		t.Fatal("released flag must survive removal")
	}

	queue.Restore("chamber-1", removed)
	got := vesselIDs(queue.List("chamber-1"))
	want := []string{"a1", "a2", "b1"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("restore must preserve FIFO order, expected %v got %v", want, got)
		}
	}

	// Restoring again must not duplicate entries.
	queue.Restore("chamber-1", removed)
	if entries := queue.List("chamber-1"); len(entries) != 3 {
		t.Fatalf("restore must be idempotent, got %d entries: %v", len(entries), vesselIDs(entries))
	}
	for _, entry := range queue.List("chamber-1") {
		if entry.Vessel.Name == "a1" && !entry.Released {
			t.Fatal("released flag must survive restore")
		}
	}
}

func TestRemovePlanNotAtHeadDoesNotChangeHead(t *testing.T) {
	queue := NewQueue()
	planA, planB := uuid.New(), uuid.New()
	queue.Add("chamber-1", planA, testVessels("a1"))
	queue.Add("chamber-1", planB, testVessels("b1"))

	queue.RemovePlan("chamber-1", planB)
	head, ok := queue.Peek("chamber-1")
	if !ok || head.Vessel.Name != "a1" {
		t.Fatalf("head must remain a1, got %+v ok=%v", head, ok)
	}
}
