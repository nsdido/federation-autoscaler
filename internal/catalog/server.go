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

package catalog

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"

	"github.com/netgroup-polito/federation-autoscaler/internal/broker/store"
)

// maxBodyBytes caps a POST body. An offer is a few hundred bytes; this only
// stops a runaway client.
const maxBodyBytes = 1 << 20

// NewHandler returns the catalog's routes over s:
//
//	POST /offers             provider publishes its offer (upsert)
//	GET  /offers             list; ?fresh=true and ?type=standard|gpu narrow it
//	GET  /offers/{clusterID} one offer, fresh or not
//
// It takes only store.OfferStore, so it cannot touch reservation counters.
// The handler is a plain http.Handler: cmd/catalog serves it on its own
// port, and it can equally be mounted on another mux.
func NewHandler(s store.OfferStore) http.Handler {
	h := &handler{store: s}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /offers", h.publish)
	mux.HandleFunc("GET /offers", h.list)
	mux.HandleFunc("GET /offers/{clusterID}", h.get)
	return mux
}

type handler struct {
	store store.OfferStore
}

func (h *handler) publish(w http.ResponseWriter, r *http.Request) {
	var req OfferRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return
	}

	stored, err := h.store.UpsertOffer(r.Context(), req.ToOffer())
	switch {
	case errors.Is(err, store.ErrPartialWrite):
		// The offer is in the catalog; only its freshness stamp is old.
		// The provider's next publish repairs it, so say so and let it retry.
		writeError(w, http.StatusServiceUnavailable, ErrCodePartialWrite, err.Error())
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, FromOffer(stored))
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	f := store.OfferFilter{ClusterType: r.URL.Query().Get("type")}
	switch v := r.URL.Query().Get("fresh"); v {
	case "", "false":
	case "true":
		f.OnlyFresh = true
	default:
		writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, `fresh must be "true" or "false"`)
		return
	}
	switch f.ClusterType {
	case "", ClusterTypeStandard, ClusterTypeGPU:
	default:
		writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, `type must be "standard" or "gpu"`)
		return
	}

	offers, err := h.store.ListOffers(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, err.Error())
		return
	}
	// The store returns no particular order; sort so the same catalog always
	// reads the same way. This is presentation, not catalog logic.
	sort.Slice(offers, func(i, j int) bool { return offers[i].ClusterID < offers[j].ClusterID })

	out := ListResponse{Offers: make([]OfferResponse, 0, len(offers))}
	for _, o := range offers {
		out.Offers = append(out.Offers, FromOffer(o))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("clusterID")
	o, err := h.store.GetOffer(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, ErrCodeNotFound, "no provider "+id+" has published an offer")
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, FromOffer(o))
}

// decodeBody reads one JSON object, rejecting unknown fields and trailing
// data. Rejecting unknown fields is what turns an attempt to send
// reservedChunks or lastSeen into a 400.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("body must contain a single JSON object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, ErrorResponse{Code: code, Message: msg})
}
