package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kranix-io/kranix-core/internal/diff"
	"github.com/kranix-io/kranix-core/internal/eventsourcing"
	"github.com/kranix-io/kranix-core/internal/resourcequota"
	"github.com/kranix-io/kranix-core/internal/scheduler"
	"github.com/kranix-io/kranix-core/internal/secretrotation"
	"github.com/kranix-io/kranix-core/internal/state"
	"github.com/kranix-io/kranix-core/internal/workloadfilter"
	"github.com/kranix-io/kranix-core/internal/workloadpage"
	"github.com/kranix-io/kranix-core/pkg/types"
	"github.com/kranix-io/kranix-packages/pagination"
)

// Server exposes a REST API for workloads, bulk ops, audit history, and secret rotation.
type Server struct {
	store      state.Store
	eventStore *eventsourcing.Store
	sched      *scheduler.Scheduler
	secrets    *secretrotation.Engine
	quota      *resourcequota.Engine
}

// New creates an HTTP API server.
func New(store state.Store, eventStore *eventsourcing.Store, sched *scheduler.Scheduler, secrets *secretrotation.Engine, quota *resourcequota.Engine) *Server {
	return &Server{store: store, eventStore: eventStore, sched: sched, secrets: secrets, quota: quota}
}

// RegisterRoutes registers core REST handlers.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/workloads/bulk", s.handleBulkWorkloads)
	mux.HandleFunc("POST /api/v1/workloads", s.handleCreateWorkload)
	mux.HandleFunc("GET /api/v1/workloads", s.handleListWorkloads)
	mux.HandleFunc("GET /api/v1/workloads/{id}/diff", s.handleWorkloadDiff)
	mux.HandleFunc("POST /api/v1/workloads/{id}/diff", s.handleWorkloadDiff)
	mux.HandleFunc("GET /api/v1/workloads/{id}", s.handleGetWorkload)
	mux.HandleFunc("GET /api/v1/quotas", s.handleListQuotas)
	mux.HandleFunc("GET /api/v1/quotas/{namespace}/usage", s.handleQuotaUsage)
	mux.HandleFunc("GET /api/v1/quotas/{namespace}", s.handleGetQuota)
	mux.HandleFunc("PUT /api/v1/quotas/{namespace}", s.handlePutQuota)
	mux.HandleFunc("DELETE /api/v1/quotas/{namespace}", s.handleDeleteQuota)
	mux.HandleFunc("DELETE /api/v1/workloads/{id}", s.handleDeleteWorkload)
	mux.HandleFunc("POST /api/v1/workloads/{id}/restart", s.handleRestartWorkload)
	mux.HandleFunc("GET /api/v1/workloads/{id}/revisions", s.handleListRevisions)
	mux.HandleFunc("POST /api/v1/workloads/{id}/rollback", s.handleRollbackWorkload)
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
	q := parseSearchQuery(r)
	ns := q.Namespace
	if q.AllNamespaces {
		ns = ""
	}
	list, err := s.store.List(r.Context(), ns)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	filtered := workloadfilter.Filter(list, q)
	pageParams := pagination.ParseParams(r.URL.Query().Get("limit"), r.URL.Query().Get("cursor"))
	page, pageInfo := workloadpage.Paginate(filtered, pageParams)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"workloads": page,
		"page_info": pageInfo,
		"count":     len(page),
		"query":     q,
	})
}

func parseSearchQuery(r *http.Request) workloadfilter.Query {
	q := r.URL.Query()
	allNS := queryFlagTrue(q, "all_namespaces", "allNamespaces", "cross_namespace", "crossNamespace")
	ns := q.Get("namespace")
	if allNS || ns == "*" {
		allNS = true
		ns = ""
	}
	return workloadfilter.Query{
		AllNamespaces: allNS,
		Namespace:     ns,
		Phase:       q.Get("phase"),
		Status:      q.Get("status"),
		Image:       q.Get("image"),
		Team:        firstNonEmpty(q.Get("team"), q.Get("tag.team")),
		Environment: firstNonEmpty(q.Get("environment"), q.Get("tag.environment")),
		CostCenter:  firstNonEmpty(q.Get("cost_center"), q.Get("costCenter"), q.Get("tag.cost_center")),
		LabelKey:    q.Get("label"),
		LabelValue:  q.Get("label_value"),
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func queryFlagTrue(q map[string][]string, keys ...string) bool {
	for _, k := range keys {
		vals, ok := q[k]
		if !ok || len(vals) == 0 {
			continue
		}
		v := strings.ToLower(strings.TrimSpace(vals[0]))
		if v == "true" || v == "1" {
			return true
		}
	}
	return false
}

func (s *Server) handleWorkloadDiff(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	wl, _ := s.store.Get(r.Context(), id)
	if wl == nil {
		writeError(w, http.StatusNotFound, "workload not found")
		return
	}
	var proposed *types.WorkloadSpec
	if r.Method == http.MethodPost {
		raw, err := io.ReadAll(r.Body)
		if err == nil && len(raw) > 0 {
			var wrap struct {
				Spec types.WorkloadSpec `json:"spec"`
			}
			if json.Unmarshal(raw, &wrap) == nil && wrap.Spec.Image != "" {
				proposed = &wrap.Spec
			} else {
				var spec types.WorkloadSpec
				if json.Unmarshal(raw, &spec) == nil {
					proposed = &spec
				}
			}
		}
	}
	result := diff.ComputeSpecVsLive(wl, proposed)
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleListQuotas(w http.ResponseWriter, r *http.Request) {
	if s.quota == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{"quotas": []types.HardResourceQuota{}, "count": 0})
		return
	}
	quotas := s.quota.ListNamespaceQuotas()
	writeJSON(w, http.StatusOK, map[string]interface{}{"quotas": quotas, "count": len(quotas)})
}

func (s *Server) handleGetQuota(w http.ResponseWriter, r *http.Request) {
	if s.quota == nil {
		writeError(w, http.StatusServiceUnavailable, "quota engine disabled")
		return
	}
	ns := r.PathValue("namespace")
	lim, ok := s.quota.GetNamespaceQuota(ns)
	if !ok {
		writeError(w, http.StatusNotFound, "quota not found")
		return
	}
	writeJSON(w, http.StatusOK, lim)
}

func (s *Server) handlePutQuota(w http.ResponseWriter, r *http.Request) {
	if s.quota == nil {
		writeError(w, http.StatusServiceUnavailable, "quota engine disabled")
		return
	}
	var lim types.HardResourceQuota
	if err := json.NewDecoder(r.Body).Decode(&lim); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	lim.Namespace = r.PathValue("namespace")
	if err := s.quota.SetNamespaceQuota(lim); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, lim)
}

func (s *Server) handleDeleteQuota(w http.ResponseWriter, r *http.Request) {
	if s.quota == nil {
		writeError(w, http.StatusServiceUnavailable, "quota engine disabled")
		return
	}
	if !s.quota.DeleteNamespaceQuota(r.PathValue("namespace")) {
		writeError(w, http.StatusNotFound, "quota not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleQuotaUsage(w http.ResponseWriter, r *http.Request) {
	if s.quota == nil {
		writeError(w, http.StatusServiceUnavailable, "quota engine disabled")
		return
	}
	usage, err := s.quota.NamespaceUsage(r.Context(), r.PathValue("namespace"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, usage)
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
