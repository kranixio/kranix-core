package server

import (
	"encoding/json"
	"net/http"

	pkgtypes "github.com/kranix-io/kranix-packages/types"
)

func (s *Server) handleMigrateWorkload(w http.ResponseWriter, r *http.Request) {
	if s.runtimeMigration == nil {
		writeError(w, http.StatusNotImplemented, "runtime migration not configured")
		return
	}
	id := r.PathValue("id")
	var req pkgtypes.WorkloadMigrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if req.WorkloadID == "" {
		req.WorkloadID = id
	}
	result, err := s.runtimeMigration.MigrateWorkload(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}
