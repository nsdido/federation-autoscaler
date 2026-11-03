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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/netgroup-polito/federation-autoscaler/internal/broker/store"
)

const asti = `{
  "clusterID": "provider-asti",
  "clusterType": "standard",
  "topology": {"region": "it-northwest", "city": "Asti", "latitude": 44.9, "longitude": 8.2},
  "resources": [
    {"name": "memory", "allocatable": "64Gi", "unitPrice": "0.004"},
    {"name": "cpu", "allocatable": "32", "unitPrice": "0.010"}
  ],
  "totalChunks": 12,
  "extra": {"note": "free text"}
}`

const milan = `{
  "clusterID": "provider-milan",
  "clusterType": "gpu",
  "resources": [{"name": "nvidia.com/gpu", "allocatable": "4"}],
  "totalChunks": 4
}`

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

func TestPublishThenRead(t *testing.T) {
	h := NewHandler(store.NewMemoryStore(time.Minute))

	rec := do(t, h, "POST", "/offers", asti)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST = %d: %s", rec.Code, rec.Body)
	}
	posted := decode[OfferResponse](t, rec)
	if posted.AvailableChunks != 12 || posted.ReservedChunks != 0 || !posted.Fresh {
		t.Errorf("unexpected counters/freshness: %+v", posted)
	}

	rec = do(t, h, "GET", "/offers/provider-asti", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET one = %d: %s", rec.Code, rec.Body)
	}
	got := decode[OfferResponse](t, rec)
	if got.Resources[0].Name != "cpu" || got.Resources[1].Name != "memory" {
		t.Errorf("resources not sorted by name: %+v", got.Resources)
	}
	if got.Topology == nil || got.Topology.Latitude == nil || *got.Topology.Latitude != 44.9 {
		t.Errorf("topology lost: %+v", got.Topology)
	}
	if got.Extra["note"] != "free text" {
		t.Errorf("extra not returned unchanged: %+v", got.Extra)
	}
	if u := got.Resources[0].UnitPrice; u == nil || *u != "0.010" {
		t.Errorf("unitPrice should round-trip as text, got %v", u)
	}
}

func TestListAndFilters(t *testing.T) {
	h := NewHandler(store.NewMemoryStore(time.Minute))
	do(t, h, "POST", "/offers", milan)
	do(t, h, "POST", "/offers", asti)

	all := decode[ListResponse](t, do(t, h, "GET", "/offers", ""))
	if len(all.Offers) != 2 || all.Offers[0].ClusterID != "provider-asti" {
		t.Fatalf("list should hold both, sorted by clusterID: %+v", all.Offers)
	}
	gpu := decode[ListResponse](t, do(t, h, "GET", "/offers?type=gpu", ""))
	if len(gpu.Offers) != 1 || gpu.Offers[0].ClusterID != "provider-milan" {
		t.Errorf("type=gpu: %+v", gpu.Offers)
	}
	if rec := do(t, h, "GET", "/offers?type=tpu", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("type=tpu = %d, want 400", rec.Code)
	}
	if rec := do(t, h, "GET", "/offers?fresh=maybe", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("fresh=maybe = %d, want 400", rec.Code)
	}
}

func TestEmptyCatalogListsAnEmptyArray(t *testing.T) {
	h := NewHandler(store.NewMemoryStore(time.Minute))
	rec := do(t, h, "GET", "/offers", "")
	if !strings.Contains(rec.Body.String(), `"offers":[]`) {
		t.Errorf("want an empty array, not null: %s", rec.Body)
	}
}

func TestStaleOfferStaysButIsNotFresh(t *testing.T) {
	// Freshness window of 1 ns: the offer is stale as soon as it is read.
	h := NewHandler(store.NewMemoryStore(time.Nanosecond))
	do(t, h, "POST", "/offers", asti)
	time.Sleep(5 * time.Millisecond)

	if l := decode[ListResponse](t, do(t, h, "GET", "/offers?fresh=true", "")); len(l.Offers) != 0 {
		t.Errorf("fresh=true should drop the stale offer: %+v", l.Offers)
	}
	one := decode[OfferResponse](t, do(t, h, "GET", "/offers/provider-asti", ""))
	if one.Fresh {
		t.Error("stale offer still reported fresh")
	}
}

func TestNotFound(t *testing.T) {
	h := NewHandler(store.NewMemoryStore(time.Minute))
	rec := do(t, h, "GET", "/offers/nobody", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("= %d, want 404", rec.Code)
	}
	if e := decode[ErrorResponse](t, rec); e.Code != ErrCodeNotFound {
		t.Errorf("code = %q", e.Code)
	}
}

func TestRejectedBodies(t *testing.T) {
	h := NewHandler(store.NewMemoryStore(time.Minute))
	cases := map[string]string{
		"not json":              `nope`,
		"two objects":           asti + asti,
		"no clusterID":          `{"clusterType":"standard","resources":[{"name":"cpu","allocatable":"1"}],"totalChunks":1}`,
		"bad clusterType":       `{"clusterID":"a","clusterType":"tpu","resources":[{"name":"cpu","allocatable":"1"}],"totalChunks":1}`,
		"no resources":          `{"clusterID":"a","clusterType":"standard","resources":[],"totalChunks":1}`,
		"duplicate resource":    `{"clusterID":"a","clusterType":"standard","resources":[{"name":"cpu","allocatable":"1"},{"name":"cpu","allocatable":"2"}],"totalChunks":1}`,
		"missing allocatable":   `{"clusterID":"a","clusterType":"standard","resources":[{"name":"cpu"}],"totalChunks":1}`,
		"missing totalChunks":   `{"clusterID":"a","clusterType":"standard","resources":[{"name":"cpu","allocatable":"1"}]}`,
		"negative totalChunks":  `{"clusterID":"a","clusterType":"standard","resources":[{"name":"cpu","allocatable":"1"}],"totalChunks":-1}`,
		"lat without lon":       `{"clusterID":"a","clusterType":"standard","topology":{"latitude":1},"resources":[{"name":"cpu","allocatable":"1"}],"totalChunks":1}`,
		"fakes reservedChunks":  `{"clusterID":"a","clusterType":"standard","resources":[{"name":"cpu","allocatable":"1"}],"totalChunks":1,"reservedChunks":0}`,
		"fakes lastSeen":        `{"clusterID":"a","clusterType":"standard","resources":[{"name":"cpu","allocatable":"1"}],"totalChunks":1,"lastSeen":"2030-01-01T00:00:00Z"}`,
		"fakes fresh":           `{"clusterID":"a","clusterType":"standard","resources":[{"name":"cpu","allocatable":"1"}],"totalChunks":1,"fresh":true}`,
		"fakes availableChunks": `{"clusterID":"a","clusterType":"standard","resources":[{"name":"cpu","allocatable":"1"}],"totalChunks":1,"availableChunks":99}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			rec := do(t, h, "POST", "/offers", body)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("= %d, want 400: %s", rec.Code, rec.Body)
			}
			if e := decode[ErrorResponse](t, rec); e.Code != ErrCodeInvalidRequest || e.Message == "" {
				t.Errorf("error body: %+v", e)
			}
		})
	}
}

// A provider republishing every 30 s must not reset what reservations hold.
func TestRepublishKeepsReservedChunks(t *testing.T) {
	ms := store.NewMemoryStore(time.Minute)
	h := NewHandler(ms)
	do(t, h, "POST", "/offers", asti)

	if ok, _, err := ms.TryConsumeChunks(context.Background(), "provider-asti", 5); err != nil || !ok {
		t.Fatalf("consume: ok=%v err=%v", ok, err)
	}
	got := decode[OfferResponse](t, do(t, h, "POST", "/offers", asti))
	if got.ReservedChunks != 5 || got.AvailableChunks != 7 {
		t.Errorf("republish reset the counter: %+v", got)
	}
}

func TestLocationUnknownIsNotZeroZero(t *testing.T) {
	h := NewHandler(store.NewMemoryStore(time.Minute))
	do(t, h, "POST", "/offers", milan) // no topology at all
	got := decode[OfferResponse](t, do(t, h, "GET", "/offers/provider-milan", ""))
	if got.Topology != nil {
		t.Errorf("unknown location must stay absent, got %+v", got.Topology)
	}

	do(t, h, "POST", "/offers", `{"clusterID":"origin","clusterType":"standard","topology":{"latitude":0,"longitude":0},"resources":[{"name":"cpu","allocatable":"1"}],"totalChunks":1}`)
	o := decode[OfferResponse](t, do(t, h, "GET", "/offers/origin", ""))
	if o.Topology == nil || o.Topology.Latitude == nil || *o.Topology.Latitude != 0 {
		t.Errorf("a real 0,0 must survive: %+v", o.Topology)
	}
}
