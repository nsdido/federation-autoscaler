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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/netgroup-polito/federation-autoscaler/internal/broker/store"
	"github.com/netgroup-polito/federation-autoscaler/internal/catalog"
)

const (
	offersPath  = "/api/v1/offers"
	astiID      = "provider-asti"
	milanID     = "provider-milan"
	typeGPU     = "gpu"
	typeStd     = "standard"
	freshWindow = time.Minute
)

// offersMux routes only the two read endpoints over a Server that has just
// a store, which is all they use. It bypasses the middleware chain on
// purpose: authentication is tested where it lives, in middleware_test.go.
func offersMux(st store.OfferStore) http.Handler {
	s := &Server{store: st}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/offers", s.handleOffersList)
	mux.HandleFunc("GET /api/v1/offers/{clusterId}", s.handleOfferGet)
	return mux
}

func getOffers(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func seedOffer(t *testing.T, st store.OfferStore, id, clusterType string, total int32) {
	t.Helper()
	_, err := st.UpsertOffer(context.Background(), store.Offer{
		ClusterID:   id,
		ClusterType: clusterType,
		Resources:   []store.Resource{{Name: "cpu", Allocatable: "32"}},
		TotalChunks: total,
	})
	if err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
}

func TestOffersListAndGet(t *testing.T) {
	st := store.NewMemoryStore(freshWindow)
	seedOffer(t, st, milanID, typeGPU, 4)
	seedOffer(t, st, astiID, typeStd, 12)
	h := offersMux(st)

	rec := getOffers(t, h, offersPath)
	if rec.Code != http.StatusOK {
		t.Fatalf("list = %d: %s", rec.Code, rec.Body)
	}
	var list catalog.ListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Offers) != 2 || list.Offers[0].ClusterID != astiID {
		t.Fatalf("want both offers sorted by clusterID, got %+v", list.Offers)
	}

	rec = getOffers(t, h, offersPath+"/"+astiID)
	var one catalog.OfferResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &one); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || one.AvailableChunks != 12 || !one.Fresh {
		t.Errorf("get = %d %+v", rec.Code, one)
	}
}

func TestOffersFilters(t *testing.T) {
	st := store.NewMemoryStore(freshWindow)
	seedOffer(t, st, astiID, typeStd, 12)
	seedOffer(t, st, milanID, typeGPU, 4)
	h := offersMux(st)

	var list catalog.ListResponse
	rec := getOffers(t, h, offersPath+"?type="+typeGPU)
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Offers) != 1 || list.Offers[0].ClusterID != milanID {
		t.Errorf("type=gpu: %+v", list.Offers)
	}

	for _, bad := range []string{"?type=tpu", "?fresh=maybe"} {
		if rec := getOffers(t, h, offersPath+bad); rec.Code != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400", bad, rec.Code)
		}
	}
}

func TestStaleOfferIsKeptButNotFresh(t *testing.T) {
	st := store.NewMemoryStore(time.Nanosecond)
	seedOffer(t, st, astiID, typeStd, 12)
	time.Sleep(5 * time.Millisecond)
	h := offersMux(st)

	var list catalog.ListResponse
	rec := getOffers(t, h, offersPath+"?fresh=true")
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Offers) != 0 {
		t.Errorf("fresh=true should drop the stale offer: %+v", list.Offers)
	}
	var one catalog.OfferResponse
	rec = getOffers(t, h, offersPath+"/"+astiID)
	if err := json.Unmarshal(rec.Body.Bytes(), &one); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || one.Fresh {
		t.Errorf("a stale offer must still be returned, as not fresh: %d %+v", rec.Code, one)
	}
}

func TestOfferNotFound(t *testing.T) {
	h := offersMux(store.NewMemoryStore(freshWindow))
	rec := getOffers(t, h, offersPath+"/nobody")
	var e ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusNotFound || e.Code != ErrCodeNotFound {
		t.Errorf("= %d %+v", rec.Code, e)
	}
}

func TestOffersWithoutStoreAre503(t *testing.T) {
	h := offersMux(nil)
	for _, p := range []string{offersPath, offersPath + "/" + astiID} {
		if rec := getOffers(t, h, p); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s = %d, want 503", p, rec.Code)
		}
	}
}
