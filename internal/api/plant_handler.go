package api

import (
	"net/http"
	"time"

	"github.com/wyw14/solo-039/internal/model"
	"github.com/wyw14/solo-039/internal/reflux"
)

func (s *Server) transition(w http.ResponseWriter, r *http.Request) {
	var input transitionRequest
	if err := decodeJSON(r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	value, err := s.plant.Transition(r.Context(), input.Grade, input.Source)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, value)
}

func (s *Server) sample(w http.ResponseWriter, r *http.Request) {
	var input sampleRequest
	if err := decodeJSON(r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if input.At == 0 {
		input.At = time.Now().Unix()
	}
	count, err := s.plant.AddSample(input.Value, input.At)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "count": count})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"count": count})
}

func (s *Server) calibrate(w http.ResponseWriter, r *http.Request) {
	revision := s.plant.CalibrateAnalyzer()
	writeJSON(w, http.StatusAccepted, map[string]any{"calibration_revision": revision})
}

func (s *Server) routeProduct(w http.ResponseWriter, r *http.Request) {
	var input routeRequest
	if err := decodeJSON(r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.plant.RouteProduct(input.Target); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"target": input.Target})
}

func (s *Server) lease(w http.ResponseWriter, r *http.Request) {
	var input leaseRequest
	if err := decodeJSON(r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	value, err := s.plant.AcquireReflux(input.Owner, input.Amount)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, value)
}

// curve 按批次阶段提交回流目标曲线；只执行当前阶段的段，其余段随阶段切换续跑。
func (s *Server) curve(w http.ResponseWriter, r *http.Request) {
	var input curveRequest
	if err := decodeJSON(r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	segments := make([]reflux.Segment, 0, len(input.Segments))
	for _, item := range input.Segments {
		segments = append(segments, reflux.Segment{
			Phase:   model.Phase(item.Phase),
			Target:  item.Target,
			Band:    item.Band,
			HoldFor: time.Duration(item.HoldMS) * time.Millisecond,
			Timeout: time.Duration(item.TimeoutMS) * time.Millisecond,
			Step:    item.Step,
		})
	}
	progress, err := s.plant.CommitRefluxCurve(r.Context(), reflux.Curve{Segments: segments})
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "progress": progress})
		return
	}
	writeJSON(w, http.StatusAccepted, progress)
}

// resumeCurve 在批次阶段切换后续跑回流曲线，保留此前各段结果。
func (s *Server) resumeCurve(w http.ResponseWriter, r *http.Request) {
	progress, err := s.plant.ResumeRefluxCurve(r.Context())
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "progress": progress})
		return
	}
	writeJSON(w, http.StatusAccepted, progress)
}

func (s *Server) shutdown(w http.ResponseWriter, r *http.Request) {
	timeout, err := boolQuery(r, "relief_timeout")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	result, err := s.plant.Shutdown(r.Context(), timeout)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "result": result})
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}
