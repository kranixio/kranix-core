package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kranix-io/kranix-core/internal/eventsourcing"
	"github.com/kranix-io/kranix-core/internal/scheduler"
	"github.com/kranix-io/kranix-core/internal/secretrotation"
	"github.com/kranix-io/kranix-core/internal/state"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Server exposes a REST API for workloads, bulk ops, audit history, and secret rotation.
type Server struct {
	store      state.Store
	eventStore *eventsourcing.Store
	sched      *scheduler.Scheduler
	secrets    *secretrotation.Engine
}

// New creates an HTTP API server.
func New(store state.Store, eventStore *eventsourcing.Store, sched *scheduler.Scheduler, secrets *secretrotation.Engine) *Server {
	return &Server{store: store, eventStore: eventStore, sched: sched, secrets: secrets}
}

// RegisterRoutes registers core REST handlers.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/workloads/bulk", s.handleBulkWorkloads)
	mux.HandleFunc("POST /api/v1/workloads", s.handleCreateWorkload)
	mux.HandleFunc("GET /api/v1/workloads", s.handleListWorkloads)
	mux.HandleFunc("GET /api/v1/workloads/{id}", s.handleGetWorkload)
	mux.HandleFunc("DELETE /api/v1/workloads/{id}", s.handleDeleteWorkload)
	mux.HandleFunc("POST /api/v1/workloads/{id}/restart", s.handleRestartWorkload)
	mux.HandleFunc("GET /api/v1/workloads/{id}/events", s.handleWorkloadEvents)
	mux.HandleFunc("GET /api/v1/events/{id}", s.handleGetEvent)
	mux.HandleFunc("GET /api/v1/audit/resources/{type}/{id}", s.handleAuditResource)
	mux.HandleFunc("POST /api/v1/secrets/rotated", s.handleSecretRotated)
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe(addr string) error {
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

func (s *Server) handleBulkWorkloads(w http.ResponseWriter, r *http.Request) {
	var req types.BulkWorkloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	resp := s.executeBulk(r.Context(), req)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) executeBulk(ctx context.Context, req types.BulkWorkloadRequest) types.BulkWorkloadResponse {
	resp := types.BulkWorkloadResponse{
		Operation: string(req.Operation),
		Results:   make([]types.BulkWorkloadResult, 0, len(req.Workloads)),
	}
	for _, item := range req.Workloads {
		res := types.BulkWorkloadResult{ID: item.ID}
		var err error
		switch req.Operation {
		case types.BulkOpDeploy:
			err = s.deployOne(ctx, item)
		case types.BulkOpRestart:
			err = s.restartOne(ctx, item.ID)
		case types.BulkOpDelete:
			err = s.store.Delete(ctx, item.ID)
		default:
			err = fmt.Errorf("unsupported operation %q", req.Operation)
		}
		if err != nil {
			res.Error = err.Error()
			resp.Failed++
			if !req.ContinueOnError {
				resp.Results = append(resp.Results, res)
				break
			}
		} else {
			res.Success = true
			resp.Succeeded++
		}
		resp.Results = append(resp.Results, res)
	}
	return resp
}

func (s *Server) deployOne(ctx context.Context, item types.BulkWorkloadItem) error {
	id := strings.TrimSpace(item.ID)
	if id == "" {
		id = fmt.Sprintf("%s-%d", strings.TrimSpace(item.Spec.Image), time.Now().UnixNano())
	}
	now := time.Now().UTC()
	w := &types.Workload{
		ID:        id,
		Name:      id,
		Namespace: "default",
		CreatedAt: now,
		UpdatedAt: now,
		Spec:      item.Spec,
		Status: types.WorkloadStatus{
			Phase:          types.WorkloadPhasePending,
			LastTransition: now,
		},
	}
	if err := s.store.Create(ctx, w); err != nil {
		if err == state.ErrWorkloadAlreadyExists {
			return s.store.Update(ctx, w)
		}
		return err
	}
	if s.secrets != nil {
		s.secrets.IndexWorkload(w)
	}
	return nil
}

func (s *Server) restartOne(ctx context.Context, id string) error {
	w, err := s.store.Get(ctx, id)
	if err != nil || w == nil {
		return fmt.Errorf("workload not found: %s", id)
	}
	if s.sched != nil {
		if err := s.sched.RequestRollingRestart(ctx, w); err != nil {
			return err
		}
	}
	w.Status.SecretRotation = clearPendingRestart(w.Status.SecretRotation)
	if s.eventStore != nil {
		_ = s.eventStore.RecordWorkloadRestarted(ctx, w, "api_restart")
	}
	return s.store.Update(ctx, w)
}

func clearPendingRestart(st *types.SecretRotationStatus) *types.SecretRotationStatus {
	if st == nil {
		return nil
	}
	st.PendingRestart = false
	st.Message = "rolling restart completed"
	return st
}

func (s *Server) handleCreateWorkload(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID   string            `json:"id"`
		Spec types.WorkloadSpec `json:"spec"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if err := s.deployOne(r.Context(), types.BulkWorkloadItem{ID: body.ID, Spec: body.Spec}); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": body.ID})
}

func (s *Server) handleListWorkloads(w http.ResponseWriter, r *http.Request) {
	ns := r.URL.Query().Get("namespace")
	list, err := s.store.List(r.Context(), ns)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleGetWorkload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	wl, _ := s.store.Get(r.Context(), id)
	if wl == nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, wl)
}

func (s *Server) handleDeleteWorkload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.Delete(r.Context(), id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRestartWorkload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.restartOne(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id, "status": "restarted"})
}

func (s *Server) handleWorkloadEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fromVersion, _ := strconv.ParseInt(r.URL.Query().Get("from_version"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if s.eventStore == nil {
		writeJSON(w, http.StatusOK, []types.DomainEvent{})
		return
	}
	events, err := s.eventStore.GetEvents(r.Context(), id, fromVersion, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.eventStore == nil {
		writeError(w, http.StatusNotFound, "event store disabled")
		return
	}
	ev, err := s.eventStore.GetEvent(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

func (s *Server) handleAuditResource(w http.ResponseWriter, r *http.Request) {
	resourceType := r.PathValue("type")
	resourceID := r.PathValue("id")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	if s.eventStore == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{"events": []types.DomainEvent{}})
		return
	}
	var events []*types.DomainEvent
	var err error
	if resourceID != "" {
		events, err = s.eventStore.GetEvents(r.Context(), resourceID, 0, limit)
	} else {
		events, err = s.eventStore.ListByResourceType(r.Context(), resourceType, limit)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"resource_type": resourceType,
		"resource_id":   resourceID,
		"events":        events,
	})
}

func (s *Server) handleSecretRotated(w http.ResponseWriter, r *http.Request) {
	if s.secrets == nil {
		writeError(w, http.StatusServiceUnavailable, "secret rotation disabled")
		return
	}
	var n secretrotation.RotationNotification
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	affected, err := s.secrets.HandleRotation(r.Context(), n)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"affected_workloads": affected,
	})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
