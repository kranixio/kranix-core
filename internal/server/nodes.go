package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	pkgtypes "github.com/kranix-io/kranix-packages/types"
)

func (s *Server) handleListNodeHealth(w http.ResponseWriter, r *http.Request) {
	if s.nodeOps != nil {
		nodes, err := s.nodeOps.ListNodeHealth(r.Context())
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		backends, _ := s.nodeOps.ListBackendHealth(r.Context())
		writeJSON(w, http.StatusOK, pkgtypes.NodeHealthListResponse{
			Nodes:    nodes,
			Backends: backends,
			Count:    len(nodes),
		})
		return
	}

	nodes, err := s.nodeHealthFromRegistry(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, pkgtypes.NodeHealthListResponse{
		Nodes: nodes,
		Count: len(nodes),
	})
}

func (s *Server) handleDrainNode(w http.ResponseWriter, r *http.Request) {
	if s.nodeOps == nil {
		writeError(w, http.StatusNotImplemented, "node drain requires runtime node operations")
		return
	}
	name := r.PathValue("name")
	var req pkgtypes.NodeDrainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && r.ContentLength > 0 {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if req.NodeName == "" {
		req.NodeName = name
	}
	result, err := s.nodeOps.DrainNode(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) nodeHealthFromRegistry(ctx context.Context) ([]pkgtypes.NodeHealthReport, error) {
	if s.sched == nil {
		return nil, nil
	}
	infos, err := s.sched.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]pkgtypes.NodeHealthReport, 0, len(infos))
	for _, n := range infos {
		score := n.HealthScore
		if score == 0 {
			score = 80
		}
		out = append(out, pkgtypes.NodeHealthReport{
			Name:          n.ID,
			Score:         score,
			Architecture:  n.Architecture,
			Ready:         !n.Draining,
			Draining:      n.Draining,
			Unschedulable: n.Unschedulable,
			CheckedAt:     now,
		})
	}
	return out, nil
}
