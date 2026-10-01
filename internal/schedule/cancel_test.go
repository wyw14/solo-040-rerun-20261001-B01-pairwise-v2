package schedule

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

var testNow = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

// failingStore wraps the in-memory journal and can force append failures for
// specific event kinds.
type failingStore struct {
	journal.Store
	failCompensated       bool
	failNthCompensated    int // 0 disables; the Nth compensated append (1-based) fails
	compensatedAppendSeen int
	failCancel            bool
}

func (s *failingStore) Append(ctx context.Context, event model.Event) (model.Event, error) {
	if event.Type == model.EventSignalChanged && strings.Contains(string(event.Payload), `"compensated":true`) {
		s.compensatedAppendSeen++
		if s.failCompensated {
			return model.Event{}, errors.New("compensation journal unavailable")
		}
		if s.failNthCompensated > 0 && s.compensatedAppendSeen == s.failNthCompensated {
			return model.Event{}, errors.New("compensation journal unavailable mid-batch")
		}
	}
	if s.failCancel && event.Type == model.EventPlanCancelled {
		return model.Event{}, errors.New("cancel event journal unavailable")
	}
	return s.Store.Append(ctx, event)
}

func newHarness(store journal.Store) (*Planner, *signal.Publisher, *signal.Store, *admission.Queue) {
	recorder := journal.NewRecorder(store, func() time.Time { return testNow })
	queue := admission.NewQueue()
	controller := admission.NewController(queue, gate.NewState("chamber-1"), signal.NewStore(), func() time.Time { return testNow })
	admissionService := admission.NewService(controller, recorder, func() time.Time { return testNow })
	planner := NewPlanner(admissionService, recorder, func() time.Time { return testNow })
	signalStore := signal.NewStore()
	publisher := signal.NewPublisher(signalStore, recorder, func() time.Time { return testNow })
	return planner, publisher, signalStore, queue
}

func vessels(names ...string) []model.Vessel {
	result := make([]model.Vessel, 0, len(names))
	for _, name := range names {
		vessel, err := model.NewVessel(name, name+"-cs", 12)
		if err != nil {
			panic(err)
		}
		result = append(result, vessel)
	}
	return result
}

func mustCreatePlan(t *testing.T, planner *Planner, planVessels []model.Vessel) model.ConvoyPlan {
	t.Helper()
	plan, err := planner.Create(context.Background(), "chamber-1", model.DirectionUpstream, planVessels)
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	return plan
}

func queueVesselNames(queue *admission.Queue) []string {
	entries := queue.List("chamber-1")
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Vessel.Name)
	}
	return names
}

func assertNames(t *testing.T, got []string, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("queue = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("queue = %v, want %v", got, want)
		}
	}
}

func publishForPlan(t *testing.T, publisher *signal.Publisher, plan model.ConvoyPlan) signal.Publication {
	t.Helper()
	cycle, err := model.NewTransitCycle("chamber-1", plan.ID, model.DirectionUpstream, 1, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(context.Background(), cycle, plan.Vessels, model.SignalProceed); err != nil {
		t.Fatalf("publish: %v", err)
	}
	return publisher.CancellationPublication(cycle.ID, cycle.Generation, model.SignalProceed, plan.Vessels)
}

// A successfully cancelled plan must leave no vessels behind in the queue, so a
// later plan is admitted instead of being blocked by the cancelled head.
func TestCancelReleasesQueueHead(t *testing.T) {
	store := &failingStore{Store: journal.NewMemoryStore()}
	planner, publisher, _, queue := newHarness(store)

	planA := mustCreatePlan(t, planner, vessels("a1", "a2"))
	if _, err := planner.Cancel(context.Background(), planA.ID, "operator cancel", publisher, signal.Publication{}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if plan, ok := planner.Get(planA.ID); !ok || !plan.Cancelled {
		t.Fatal("plan must be marked cancelled")
	}
	if len(queue.List("chamber-1")) != 0 {
		t.Fatalf("cancelled vessels must be removed from queue, got %v", queueVesselNames(queue))
	}

	planB := mustCreatePlan(t, planner, vessels("b1"))
	head, ok := queue.Peek("chamber-1")
	if !ok || head.PlanID != planB.ID {
		t.Fatalf("new plan must be at queue head, got %+v ok=%v", head, ok)
	}
}

// Cancelling also clears the published proceed signals of the plan's vessels.
func TestCancelCompensatesPublishedSignals(t *testing.T) {
	store := &failingStore{Store: journal.NewMemoryStore()}
	planner, publisher, signalStore, _ := newHarness(store)

	plan := mustCreatePlan(t, planner, vessels("a1", "a2"))
	publication := publishForPlan(t, publisher, plan)
	if len(publication.Published) != 2 {
		t.Fatalf("expected 2 captured signals, got %d", len(publication.Published))
	}
	if _, err := planner.Cancel(context.Background(), plan.ID, "operator cancel", publisher, publication); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	for _, vessel := range plan.Vessels {
		if _, ok := signalStore.Get(vessel.ID); ok {
			t.Fatalf("signal for %s must be cleared after cancellation", vessel.Name)
		}
	}
}

// When compensation cannot be journaled the whole cancellation must leave the
// system in its pre-cancel state, and a later retry with the same publication
// must succeed.
func TestCancelCompensationFailureKeepsOriginalStateAndIsRetryable(t *testing.T) {
	store := &failingStore{Store: journal.NewMemoryStore(), failCompensated: true}
	planner, publisher, signalStore, queue := newHarness(store)

	planA := mustCreatePlan(t, planner, vessels("a1", "a2"))
	mustCreatePlan(t, planner, vessels("b1"))
	publication := publishForPlan(t, publisher, planA)

	if _, err := planner.Cancel(context.Background(), planA.ID, "operator cancel", publisher, publication); err == nil {
		t.Fatal("expected cancellation to fail while compensation journal is down")
	}
	if plan, ok := planner.Get(planA.ID); !ok || plan.Cancelled {
		t.Fatal("plan must remain active after failed cancellation")
	}
	assertNames(t, queueVesselNames(queue), []string{"a1", "a2", "b1"})
	for _, vessel := range planA.Vessels {
		if state, ok := signalStore.Get(vessel.ID); !ok || state.Aspect != model.SignalProceed {
			t.Fatalf("signal for %s must remain proceed after failed cancellation", vessel.Name)
		}
	}

	store.failCompensated = false
	if _, err := planner.Cancel(context.Background(), planA.ID, "operator cancel", publisher, publication); err != nil {
		t.Fatalf("retry cancel: %v", err)
	}
	plan, _ := planner.Get(planA.ID)
	if !plan.Cancelled {
		t.Fatal("plan must be cancelled after successful retry")
	}
	assertNames(t, queueVesselNames(queue), []string{"b1"})
	for _, vessel := range planA.Vessels {
		if _, ok := signalStore.Get(vessel.ID); ok {
			t.Fatalf("signal for %s must be cleared after retry", vessel.Name)
		}
	}
}

// A compensation that clears one vessel and then fails must not lose progress:
// the retry skips the already-cleared vessel and clears the rest.
func TestCancelCompensationPartialFailureRetriesFromProgress(t *testing.T) {
	store := &failingStore{Store: journal.NewMemoryStore(), failNthCompensated: 2}
	planner, publisher, signalStore, queue := newHarness(store)

	plan := mustCreatePlan(t, planner, vessels("a1", "a2"))
	publication := publishForPlan(t, publisher, plan)

	if _, err := planner.Cancel(context.Background(), plan.ID, "operator cancel", publisher, publication); err == nil {
		t.Fatal("expected failure when second compensation event cannot be recorded")
	}
	// One vessel signal was cleared journal-first before the failure; the
	// queue must still be fully restored.
	assertNames(t, queueVesselNames(queue), []string{"a1", "a2"})
	cleared := 0
	for _, vessel := range plan.Vessels {
		if _, ok := signalStore.Get(vessel.ID); !ok {
			cleared++
		}
	}
	if cleared != 1 {
		t.Fatalf("expected exactly 1 signal cleared before failure, got %d", cleared)
	}

	store.failNthCompensated = 0
	if _, err := planner.Cancel(context.Background(), plan.ID, "operator cancel", publisher, publication); err != nil {
		t.Fatalf("retry cancel: %v", err)
	}
	if got, _ := planner.Get(plan.ID); !got.Cancelled {
		t.Fatal("plan must be cancelled after retry")
	}
	for _, vessel := range plan.Vessels {
		if _, ok := signalStore.Get(vessel.ID); ok {
			t.Fatalf("signal for %s must be cleared after retry", vessel.Name)
		}
	}
	if len(queue.List("chamber-1")) != 0 {
		t.Fatalf("queue must be empty, got %v", queueVesselNames(queue))
	}
}

// Repeating a committed cancellation is idempotent and does not re-add vessels.
func TestCancelIsIdempotent(t *testing.T) {
	store := &failingStore{Store: journal.NewMemoryStore()}
	planner, publisher, _, queue := newHarness(store)

	plan := mustCreatePlan(t, planner, vessels("a1"))
	if _, err := planner.Cancel(context.Background(), plan.ID, "operator cancel", publisher, signal.Publication{}); err != nil {
		t.Fatal(err)
	}
	result, err := planner.Cancel(context.Background(), plan.ID, "operator cancel", publisher, signal.Publication{})
	if err != nil {
		t.Fatalf("second cancel must be idempotent: %v", err)
	}
	if !result.Plan.Cancelled || len(queue.List("chamber-1")) != 0 {
		t.Fatal("idempotent cancel must keep plan cancelled and queue empty")
	}
}

// If the plan-cancelled event itself cannot be recorded, both the queue and
// field signals are rolled back to the pre-cancel state.
func TestCancelRecordFailureRollsBackSignalsAndQueue(t *testing.T) {
	store := &failingStore{Store: journal.NewMemoryStore(), failCancel: true}
	planner, publisher, signalStore, queue := newHarness(store)

	plan := mustCreatePlan(t, planner, vessels("a1"))
	publication := publishForPlan(t, publisher, plan)

	if _, err := planner.Cancel(context.Background(), plan.ID, "operator cancel", publisher, publication); err == nil {
		t.Fatal("expected failure when cancel event cannot be recorded")
	}
	if got, _ := planner.Get(plan.ID); got.Cancelled {
		t.Fatal("plan must remain active")
	}
	assertNames(t, queueVesselNames(queue), []string{"a1"})
	if state, ok := signalStore.Get(plan.Vessels[0].ID); !ok || state.Aspect != model.SignalProceed {
		t.Fatal("published signal must be restored after cancel-record rollback")
	}

	store.failCancel = false
	if _, err := planner.Cancel(context.Background(), plan.ID, "operator cancel", publisher, publication); err != nil {
		t.Fatalf("retry cancel: %v", err)
	}
	if got, _ := planner.Get(plan.ID); !got.Cancelled {
		t.Fatal("plan must be cancelled after retry")
	}
	if len(queue.List("chamber-1")) != 0 {
		t.Fatalf("queue must be empty, got %v", queueVesselNames(queue))
	}
}
