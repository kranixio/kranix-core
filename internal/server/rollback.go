package server

import (
	"encoding/json"
	"net/http"

	"github.com/kranix-io/kranix-core/internal/rollouthistory"
	"github.com/kranix-io/kranix-core/internal/state"
	"github.com/kranix-io/kranix-core/pkg/types"
	pkgtypes "github.com/kranix-io/kranix-packages/types"
)

func (s *Server) handleListRevisions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	revisions, err := rollouthistory.ListRevisions(r.Context(), s.store, id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	wl, _ := s.store.Get(r.Context(), id)
	pkgRevisions := convertRevisionsToPackages(revisions)
	resp := pkgtypes.RevisionListResponse{
		WorkloadID: id,
		Revisions:  pkgRevisions,
		Count:      len(pkgRevisions),
	}
	if wl != nil {
		resp.Namespace = wl.Namespace
	}
	writeJSON(w, http.StatusOK, resp)
}

func convertRevisionsToPackages(revisions []types.WorkloadRevision) []pkgtypes.WorkloadRevision {
	data, err := json.Marshal(revisions)
	if err != nil {
		return nil
	}
	var out []pkgtypes.WorkloadRevision
	_ = json.Unmarshal(data, &out)
	return out
}

func (s *Server) handleRollbackWorkload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req pkgtypes.RollbackRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	wl, err := s.store.Get(r.Context(), id)
	if err != nil || wl == nil {
		writeError(w, http.StatusNotFound, "workload not found")
		return
	}

	revisionID := req.RevisionID
	if revisionID == "" {
		if len(wl.RollbackVersions) < 2 {
			writeError(w, http.StatusBadRequest, "no previous revision available for rollback")
			return
		}
		revisionID = wl.RollbackVersions[1].ID
	}

	previousImage := wl.Spec.Image
	updated, err := rollouthistory.Revert(r.Context(), s.store, id, revisionID, s.rollbackMaxVersions())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, pkgtypes.RollbackResult{
		WorkloadID:   id,
		Namespace:    updated.Namespace,
		RevisionID:   revisionID,
		PreviousSpec: previousImage,
		RestoredSpec: updated.Spec.Image,
		Status:       "rolled_back",
		Message:      "workload reverted to selected revision",
	})
}

func (s *Server) rollbackMaxVersions() int {
	if vs, ok := s.store.(*state.VersionedStore); ok {
		return vs.MaxVersions()
	}
	return 10
}
