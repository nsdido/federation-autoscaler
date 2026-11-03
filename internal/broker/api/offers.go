/*
Copyright 2026 Politecnico di Torino - NetGroup.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package api

import (
	"errors"
	"net/http"
	"sort"

	"github.com/netgroup-polito/federation-autoscaler/internal/broker/store"
	"github.com/netgroup-polito/federation-autoscaler/internal/catalog"
)

// This file is the read side of the catalog. The write side already exists:
// POST /api/v1/advertisements records every advertisement in the Store (see
// handleAdvertisementUpsert), so providers need no new endpoint. What was
// missing was a way for a reader, such as the Brain, to get the catalog back
// out. These two handlers are that way.
//
// Both only read from s.store and never touch a custom resource, so they
// cannot change what the reconcilers see. They return the same JSON shape as
// the standalone catalog service (internal/catalog), built by the same
// conversion function, so there is one wire format and not two.

// handleOffersList serves GET /api/v1/offers.
//
// Query parameters, both optional:
//
//	fresh=true            keep only providers still within the freshness window
//	type=standard|gpu     keep only offers of that cluster type
//
// A provider that has gone quiet is not removed from the catalog: without
// fresh=true it still appears, with "fresh": false.
func (s *Server) handleOffersList(w http.ResponseWriter, r *http.Request) {
	requestID := r.Header.Get(HeaderRequestID)
	if !s.requireStore(w, requestID) {
		return
	}

	q := r.URL.Query()
	filter := store.OfferFilter{ClusterType: q.Get("type")}
	switch q.Get("fresh") {
	case "", "false":
	case "true":
		filter.OnlyFresh = true
	default:
		writeError(w, http.StatusBadRequest, ErrorResponse{
			Code: ErrCodeInvalidRequest, Message: `fresh must be "true" or "false"`, RequestID: requestID,
		})
		return
	}
	switch filter.ClusterType {
	case "", catalog.ClusterTypeStandard, catalog.ClusterTypeGPU:
	default:
		writeError(w, http.StatusBadRequest, ErrorResponse{
			Code: ErrCodeInvalidRequest, Message: `type must be "standard" or "gpu"`, RequestID: requestID,
		})
		return
	}

	offers, err := s.store.ListOffers(r.Context(), filter)
	if err != nil {
		s.log.Error(err, "list offers failed", "requestId", requestID)
		writeError(w, http.StatusInternalServerError, ErrorResponse{
			Code: ErrCodeInternalError, Message: "catalog read failed", RequestID: requestID,
		})
		return
	}
	// The store returns no particular order. Sorting here keeps the answer
	// stable between two reads; it is presentation, not catalog logic.
	sort.Slice(offers, func(i, j int) bool { return offers[i].ClusterID < offers[j].ClusterID })

	out := catalog.ListResponse{Offers: make([]catalog.OfferResponse, 0, len(offers))}
	for _, o := range offers {
		out.Offers = append(out.Offers, catalog.FromOffer(o))
	}
	writeJSON(w, http.StatusOK, out)
}

// handleOfferGet serves GET /api/v1/offers/{clusterId}. It returns the offer
// whether or not it is fresh, so a reader can tell "quiet" from "unknown".
func (s *Server) handleOfferGet(w http.ResponseWriter, r *http.Request) {
	requestID := r.Header.Get(HeaderRequestID)
	if !s.requireStore(w, requestID) {
		return
	}

	id := r.PathValue("clusterId")
	o, err := s.store.GetOffer(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, ErrorResponse{
			Code: ErrCodeNotFound, Message: "no provider " + id + " has advertised", RequestID: requestID,
		})
		return
	case err != nil:
		s.log.Error(err, "get offer failed", "clusterId", id, "requestId", requestID)
		writeError(w, http.StatusInternalServerError, ErrorResponse{
			Code: ErrCodeInternalError, Message: "catalog read failed", RequestID: requestID,
		})
		return
	}
	writeJSON(w, http.StatusOK, catalog.FromOffer(o))
}

// requireStore answers 503 and returns false when no Store is wired in. The
// store is optional (ServerOptions.Store), so a Broker started without one
// has no catalog to read, and saying so is more useful than an empty list
// that looks like "no providers".
func (s *Server) requireStore(w http.ResponseWriter, requestID string) bool {
	if s.store != nil {
		return true
	}
	writeError(w, http.StatusServiceUnavailable, ErrorResponse{
		Code: ErrCodeServiceUnavailable, Message: "catalog store is not configured", RequestID: requestID,
	})
	return false
}
