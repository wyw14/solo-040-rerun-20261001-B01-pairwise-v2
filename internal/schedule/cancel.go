package schedule

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

type Cancellation struct {
	Plan         model.ConvoyPlan `json:"plan"`
	RemovedQueue int              `json:"removed_queue"`
	Compensated  int              `json:"compensated"`
}

// Cancel reverts queue and field-signal effects as one observable operation.
//
// Saga ordering:
//  1. detach the plan's vessels from the admission queue;
//  2. compensate the published field signals (journal-first, idempotent);
//  3. record the plan-cancelled event and only then mark the plan cancelled.
//
// On any failure both sides are restored to their pre-cancel state and the
// guard is released, so the exact same call can be retried: a vessel whose
// signal was already compensated is skipped, and queue entries return to their
// original FIFO positions.
func (p *Planner) Cancel(ctx context.Context, planID uuid.UUID, note string, publisher *signal.Publisher, publication signal.Publication) (Cancellation, error) {
	plan, err := p.beginCancel(planID)
	if err != nil {
		return Cancellation{}, err
	}
	if plan.Cancelled {
		// A previous call already committed; cancellation is idempotent.
		return Cancellation{Plan: plan}, nil
	}
	defer p.endCancel(planID)

	removed := p.admission.RemovePlan(plan.ChamberID, plan)

	if err := publisher.ApplyCompensation(context.WithoutCancel(ctx), plan.ChamberID, publication); err != nil {
		// Field signals could not be fully reverted: put the vessels back in
		// their original queue positions so the plan remains admissible and
		// the call can be retried.
		p.admission.Restore(plan.ChamberID, removed)
		return Cancellation{}, errors.Join(errors.New("signal compensation failed"), err)
	}

	cancelled := plan.Clone()
	cancelled.Cancelled = true
	cancelled.CancelNote = note
	if _, err := p.recorder.Record(context.WithoutCancel(ctx), plan.ChamberID, model.EventPlanCancelled, cancelled.ID, 0, map[string]any{
		"note": note, "removed_queue": len(removed), "compensated_signals": len(publication.Published),
	}); err != nil {
		// The cancellation could not be durably committed. Roll both effects
		// back to the pre-cancel state; a retry will converge because both
		// compensation and queue restore are idempotent.
		reapplyErr := publisher.Reapply(context.WithoutCancel(ctx), plan.ChamberID, publication)
		p.admission.Restore(plan.ChamberID, removed)
		return Cancellation{}, errors.Join(errors.New("record plan cancellation failed"), err, reapplyErr)
	}

	p.replace(cancelled)
	return Cancellation{
		Plan: cancelled, RemovedQueue: len(removed), Compensated: len(publication.Published),
	}, nil
}

// beginCancel serializes concurrent cancellations of the same plan and returns
// the live plan state.
func (p *Planner) beginCancel(planID uuid.UUID) (model.ConvoyPlan, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	plan, ok := p.plans[planID]
	if !ok {
		return model.ConvoyPlan{}, errors.New("convoy plan not found")
	}
	if plan.Cancelled {
		return plan.Clone(), nil
	}
	if p.cancelling[planID] {
		return model.ConvoyPlan{}, errors.New("plan cancellation is already in progress")
	}
	p.cancelling[planID] = true
	return plan.Clone(), nil
}

func (p *Planner) endCancel(planID uuid.UUID) {
	p.mu.Lock()
	delete(p.cancelling, planID)
	p.mu.Unlock()
}
