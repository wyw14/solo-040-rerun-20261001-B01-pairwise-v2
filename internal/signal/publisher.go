package signal

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type Publication struct {
	CycleID    uuid.UUID
	Generation uint64
	Aspect     model.SignalAspect
	Published  []uuid.UUID
	Previous   map[uuid.UUID]model.SignalState
}

// Publisher applies a convoy signal change with a compensatable record.
type Publisher struct {
	store    *Store
	recorder *journal.Recorder
	now      func() time.Time
}

func NewPublisher(store *Store, recorder *journal.Recorder, now func() time.Time) *Publisher {
	if now == nil {
		now = time.Now
	}
	return &Publisher{store: store, recorder: recorder, now: now}
}

func (p *Publisher) Publish(ctx context.Context, cycle model.TransitCycle, vessels []model.Vessel, aspect model.SignalAspect) (Publication, error) {
	publication := Publication{
		CycleID: cycle.ID, Generation: cycle.Generation, Aspect: aspect,
		Previous: make(map[uuid.UUID]model.SignalState),
	}
	for _, vessel := range vessels {
		if err := ctx.Err(); err != nil {
			_ = p.ApplyCompensation(context.WithoutCancel(ctx), cycle.ChamberID, publication)
			return publication, err
		}
		if previous, ok := p.store.Get(vessel.ID); ok {
			publication.Previous[vessel.ID] = previous
		}
		state := p.store.Set(vessel.ID, cycle.ID, cycle.Generation, aspect, p.now())
		publication.Published = append(publication.Published, vessel.ID)
		if _, err := p.recorder.Record(ctx, cycle.ChamberID, model.EventSignalChanged, cycle.ID, cycle.Generation, state); err != nil {
			_ = p.ApplyCompensation(context.WithoutCancel(ctx), cycle.ChamberID, publication)
			return publication, err
		}
	}
	return publication, nil
}

// CancellationPublication captures which of a plan's vessels currently hold a
// field signal. Cancellation compensation clears those signals (they did not
// exist before the plan published them); Aspect records what Reapply must
// restore if the cancellation has to be rolled back. The snapshot must be
// taken BEFORE any signals are cleared, so a cancellation that fails after
// clearing some signals can be retried even though those vessels have already
// disappeared from the store.
func (p *Publisher) CancellationPublication(cycleID uuid.UUID, generation uint64, aspect model.SignalAspect, vessels []model.Vessel) Publication {
	publication := Publication{
		CycleID: cycleID, Generation: generation, Aspect: aspect,
		Previous: make(map[uuid.UUID]model.SignalState),
	}
	for _, vessel := range vessels {
		if _, ok := p.store.Get(vessel.ID); ok {
			publication.Published = append(publication.Published, vessel.ID)
		}
	}
	return publication
}

// ApplyCompensation reverts a published signal set. Every vessel is journaled
// before its in-memory signal is touched, and vessels that are already in their
// compensated state are skipped. Consequently:
//   - a journal failure leaves the remaining field signals on their published
//     aspect and the exact same publication can be retried until it succeeds;
//   - re-applying a completed compensation is a safe no-op that emits no
//     duplicate events.
func (p *Publisher) ApplyCompensation(ctx context.Context, chamberID string, publication Publication) error {
	for index := len(publication.Published) - 1; index >= 0; index-- {
		vesselID := publication.Published[index]
		previous, hadPrevious := publication.Previous[vesselID]
		if vesselAlreadyCompensated(p.store, vesselID, previous, hadPrevious) {
			continue
		}
		if _, err := p.recorder.Record(ctx, chamberID, model.EventSignalChanged, publication.CycleID, publication.Generation, map[string]any{
			"vessel_id": vesselID, "compensated": true,
		}); err != nil {
			return errors.Join(errors.New("signal compensation journal failed"), err)
		}
		if hadPrevious {
			p.store.Set(previous.VesselID, previous.CycleID, previous.Generation, previous.Aspect, p.now())
		} else {
			p.store.Remove(vesselID)
		}
	}
	return nil
}

// Reapply puts the published signals back after they had to be rolled back
// (for example when the cancellation itself could not be recorded). It restores
// the exact published aspect for every vessel and is safe to run repeatedly:
// re-setting an already republished signal changes nothing on the field.
func (p *Publisher) Reapply(ctx context.Context, chamberID string, publication Publication) error {
	aspect := publication.Aspect
	if aspect == "" {
		aspect = model.SignalProceed
	}
	var combined error
	for _, vesselID := range publication.Published {
		state := p.store.Set(vesselID, publication.CycleID, publication.Generation, aspect, p.now())
		if _, err := p.recorder.Record(ctx, chamberID, model.EventSignalChanged, publication.CycleID, publication.Generation, state); err != nil {
			combined = errors.Join(combined, err)
		}
	}
	return combined
}

func vesselAlreadyCompensated(store *Store, vesselID uuid.UUID, previous model.SignalState, hadPrevious bool) bool {
	current, exists := store.Get(vesselID)
	if !hadPrevious {
		return !exists
	}
	return exists && current.VesselID == previous.VesselID &&
		current.CycleID == previous.CycleID && current.Generation == previous.Generation &&
		current.Aspect == previous.Aspect
}
