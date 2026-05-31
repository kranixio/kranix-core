package server

import (
	"encoding/json"
	"net/http"

	pkgtypes "github.com/kranix-io/kranix-packages/types"
)

func (s *Server) handleCheckpointWorkload(w http.ResponseWriter, r *http.Request) {
	if s.runtimeOps == nil {
		writeError(w, http.StatusNotImplemented, "runtime extended operations not configured")
		return
	}
	id := r.PathValue("id")
	var req pkgtypes.CheckpointRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
	}
	if req.WorkloadID == "" {
		req.WorkloadID = id
	}
	result, err := s.runtimeOps.CheckpointWorkload(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleRestoreWorkload(w http.ResponseWriter, r *http.Request) {
	if s.runtimeOps == nil {
		writeError(w, http.StatusNotImplemented, "runtime extended operations not configured")
		return
	}
	id := r.PathValue("id")
	var req pkgtypes.RestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if req.WorkloadID == "" {
		req.WorkloadID = id
	}
	result, err := s.runtimeOps.RestoreWorkload(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleListCheckpoints(w http.ResponseWriter, r *http.Request) {
	if s.runtimeOps == nil {
		writeJSON(w, http.StatusOK, []pkgtypes.CheckpointResult{})
		return
	}
	id := r.PathValue("id")
	namespace := r.URL.Query().Get("namespace")
	list, err := s.runtimeOps.ListCheckpoints(r.Context(), id, namespace)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleListRuntimePlugins(w http.ResponseWriter, r *http.Request) {
	if s.runtimePlugins == nil {
		writeJSON(w, http.StatusOK, pkgtypes.RuntimePluginListResponse{Plugins: []pkgtypes.RuntimePluginInfo{}, Count: 0})
		return
	}
	plugins := s.runtimePlugins()
	writeJSON(w, http.StatusOK, pkgtypes.RuntimePluginListResponse{Plugins: plugins, Count: len(plugins)})
}
