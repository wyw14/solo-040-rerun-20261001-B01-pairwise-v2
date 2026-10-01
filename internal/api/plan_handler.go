package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

type vesselInput struct {
	Name     string  `json:"name"`
	CallSign string  `json:"call_sign"`
	LengthM  float64 `json:"length_m"`
}

type planInput struct {
	ChamberID string          `json:"chamber_id"`
	Direction model.Direction `json:"direction"`
	Vessels   []vesselInput   `json:"vessels"`
}

func createPlanHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input planInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		vessels := make([]model.Vessel, 0, len(input.Vessels))
		for _, item := range input.Vessels {
			vessel, err := model.NewVessel(item.Name, item.CallSign, item.LengthM)
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			vessels = append(vessels, vessel)
		}
		plan, err := runtime.Schedule.Create(r.Context(), input.ChamberID, input.Direction, vessels)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		cycleValue, err := runtime.CycleService.Start(r.Context(), plan)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"plan": plan, "cycle": cycleValue})
	}
}

func plansHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := make(map[string]any, len(runtime.Chambers))
		for _, chamberID := range runtime.Chambers {
			result[chamberID] = runtime.Schedule.Active(chamberID)
		}
		writeJSON(w, http.StatusOK, result)
	}
}

func publishSignalsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		planID, err := uuid.Parse(chi.URLParam(r, "planID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		plan, ok := runtime.Planner.Get(planID)
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("plan not found"))
			return
		}
		cycleValue, ok := runtime.Cycles.Current(plan.ChamberID)
		if !ok {
			writeError(w, http.StatusConflict, errors.New("active transit not found"))
			return
		}
		publication, err := runtime.Schedule.Publish(r.Context(), planID, cycleValue)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusAccepted, publication)
	}
}

func cancelPlanHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		planID, err := uuid.Parse(chi.URLParam(r, "planID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		plan, ok := runtime.Planner.Get(planID)
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("plan not found"))
			return
		}
		// Capture the live field signals before any state changes so that a
		// compensation that fails partway can still be retried against the
		// exact pre-cancel snapshot, even if the cycle is gone afterwards.
		cycleID, generation := uuid.Nil, uint64(0)
		if cycleValue, cycleOK := runtime.Cycles.Current(plan.ChamberID); cycleOK {
			cycleID, generation = cycleValue.ID, cycleValue.Generation
		}
		publication := runtime.SignalPublisher.CancellationPublication(cycleID, generation, model.SignalProceed, plan.Vessels)
		result, err := runtime.Schedule.Cancel(r.Context(), planID, "operator cancellation", publication)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		if _, cycleOK := runtime.Cycles.Current(plan.ChamberID); cycleOK {
			_, _ = runtime.Cycles.Cancel(r.Context(), plan.ChamberID, "operator cancellation")
		}
		writeJSON(w, http.StatusOK, result)
	}
}
