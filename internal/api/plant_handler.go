package api

import (
	"net/http"
	"time"

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

func (s *Server) submitCurve(w http.ResponseWriter, r *http.Request) {
	var input curveRequest
	if err := decodeJSON(r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if input.Segments == nil {
		input.Segments = []reflux.CurveSegment{}
	}
	status, err := s.plant.SubmitRefluxCurve(input.Segments, input.Extend)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "status": status})
		return
	}
	// drive=false 时只提交目标曲线，不替调用方跨越任何控制步骤。
	if input.Drive {
		status, err = s.plant.DriveRefluxSegment(r.Context())
		if err != nil {
			writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "status": status})
			return
		}
	}
	writeJSON(w, http.StatusAccepted, status)
}

func (s *Server) advanceCurve(w http.ResponseWriter, r *http.Request) {
	// 当前段未达标时这里会失败并留在当前段，错误信息即留段原因。
	status, err := s.plant.AdvanceRefluxSegment()
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "status": status})
		return
	}
	writeJSON(w, http.StatusAccepted, status)
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
