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

// Package catalog is the HTTP face of the offer catalog: the wire format a
// provider publishes its offer in, and the handlers that put it into a
// store.OfferStore and read it back out.
//
// It knows nothing about Kubernetes or about which backend holds the data.
// Everything it stores and returns goes through store.OfferStore.
package catalog

import (
	"errors"
	"fmt"
	"time"

	"github.com/netgroup-polito/federation-autoscaler/internal/broker/store"
)

// Cluster types the catalog accepts.
const (
	ClusterTypeStandard = "standard"
	ClusterTypeGPU      = "gpu"
)

// TopologyWire is where a provider is. Latitude and Longitude are pointers so
// that "at 0,0" and "location unknown" stay different things on the wire,
// exactly as store.Topology.HasLocation keeps them different in Go.
type TopologyWire struct {
	Zone      string   `json:"zone,omitempty"`
	Region    string   `json:"region,omitempty"`
	City      string   `json:"city,omitempty"`
	Latitude  *float64 `json:"latitude,omitempty"`
	Longitude *float64 `json:"longitude,omitempty"`
}

// ResourceWire is one resource a provider offers. Quantities are text
// ("32", "64Gi") and are stored unchanged.
type ResourceWire struct {
	Name         string  `json:"name"`
	Allocatable  string  `json:"allocatable"`
	UnitPrice    *string `json:"unitPrice,omitempty"`
	ScalePercent *int32  `json:"scalePercent,omitempty"`
	FixedCap     *string `json:"fixedCap,omitempty"`
}

// OfferRequest is what a provider POSTs to publish its offer.
//
// There is deliberately no field for reservedChunks, availableChunks,
// lastSeen or fresh. The store owns those, and the decoder rejects a body
// that names an unknown field, so a provider that tries to send one gets a
// 400 instead of being silently ignored.
type OfferRequest struct {
	ClusterID     string `json:"clusterID"`
	LiqoClusterID string `json:"liqoClusterID,omitempty"`
	ClusterType   string `json:"clusterType"`

	Topology  *TopologyWire  `json:"topology,omitempty"`
	Resources []ResourceWire `json:"resources"`

	ProbeEndpoint   string    `json:"probeEndpoint,omitempty"`
	CarbonIntensity *float64  `json:"carbonIntensity,omitempty"`
	CarbonForecast  []float64 `json:"carbonForecast,omitempty"`
	Renewable       bool      `json:"renewable,omitempty"`

	// TotalChunks is a pointer so that a missing field is an error rather
	// than a provider that quietly offers zero chunks.
	TotalChunks *int32 `json:"totalChunks"`

	// Extra carries anything the catalog does not define. The store keeps it
	// and hands it back unchanged; it is never looked into.
	Extra map[string]string `json:"extra,omitempty"`
}

// OfferResponse is what a reader gets back: the request fields, plus the
// four values only the store may set.
type OfferResponse struct {
	ClusterID     string `json:"clusterID"`
	LiqoClusterID string `json:"liqoClusterID,omitempty"`
	ClusterType   string `json:"clusterType"`

	Topology  *TopologyWire  `json:"topology,omitempty"`
	Resources []ResourceWire `json:"resources"`

	ProbeEndpoint   string    `json:"probeEndpoint,omitempty"`
	CarbonIntensity *float64  `json:"carbonIntensity,omitempty"`
	CarbonForecast  []float64 `json:"carbonForecast,omitempty"`
	Renewable       bool      `json:"renewable"`

	TotalChunks     int32 `json:"totalChunks"`
	ReservedChunks  int32 `json:"reservedChunks"`
	AvailableChunks int32 `json:"availableChunks"`

	LastSeen time.Time `json:"lastSeen"`
	Fresh    bool      `json:"fresh"`

	Extra map[string]string `json:"extra,omitempty"`
}

// ListResponse is the body of GET /offers.
type ListResponse struct {
	Offers []OfferResponse `json:"offers"`
}

// ErrorResponse is the body of every non-2xx answer.
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error codes.
const (
	ErrCodeInvalidRequest = "invalid_request"
	ErrCodeNotFound       = "not_found"
	ErrCodePartialWrite   = "partial_write"
	ErrCodeInternal       = "internal"
)

// Validate checks the request and returns the first problem it finds.
func (r *OfferRequest) Validate() error {
	if r.ClusterID == "" {
		return errors.New("clusterID is required")
	}
	switch r.ClusterType {
	case ClusterTypeStandard, ClusterTypeGPU:
	case "":
		return errors.New("clusterType is required")
	default:
		return fmt.Errorf("clusterType %q is not one of %q, %q",
			r.ClusterType, ClusterTypeStandard, ClusterTypeGPU)
	}
	if len(r.Resources) == 0 {
		return errors.New("resources must contain at least one resource")
	}
	seen := make(map[string]struct{}, len(r.Resources))
	for i, res := range r.Resources {
		if res.Name == "" {
			return fmt.Errorf("resources[%d].name is required", i)
		}
		if res.Allocatable == "" {
			return fmt.Errorf("resources[%d] (%s): allocatable is required", i, res.Name)
		}
		if _, dup := seen[res.Name]; dup {
			return fmt.Errorf("resource %q appears more than once", res.Name)
		}
		seen[res.Name] = struct{}{}
	}
	if r.TotalChunks == nil {
		return errors.New("totalChunks is required")
	}
	if *r.TotalChunks < 0 {
		return errors.New("totalChunks must not be negative")
	}
	if t := r.Topology; t != nil && (t.Latitude == nil) != (t.Longitude == nil) {
		return errors.New("topology: latitude and longitude must be given together")
	}
	return nil
}

// ToOffer converts a validated request into the store's type. It never sets
// ReservedChunks, LastSeen or Fresh: those belong to the store.
func (r *OfferRequest) ToOffer() store.Offer {
	o := store.Offer{
		ClusterID:       r.ClusterID,
		LiqoClusterID:   r.LiqoClusterID,
		ClusterType:     r.ClusterType,
		ProbeEndpoint:   r.ProbeEndpoint,
		CarbonIntensity: r.CarbonIntensity,
		CarbonForecast:  r.CarbonForecast,
		Renewable:       r.Renewable,
		Extra:           r.Extra,
	}
	if r.TotalChunks != nil {
		o.TotalChunks = *r.TotalChunks
	}
	if t := r.Topology; t != nil {
		o.Topology = store.Topology{Zone: t.Zone, Region: t.Region, City: t.City}
		if t.Latitude != nil && t.Longitude != nil {
			o.Topology.Latitude = *t.Latitude
			o.Topology.Longitude = *t.Longitude
			o.Topology.HasLocation = true
		}
	}
	o.Resources = make([]store.Resource, 0, len(r.Resources))
	for _, res := range r.Resources {
		o.Resources = append(o.Resources, store.Resource{
			Name:         res.Name,
			Allocatable:  res.Allocatable,
			UnitPrice:    res.UnitPrice,
			ScalePercent: res.ScalePercent,
			FixedCap:     res.FixedCap,
		})
	}
	return o
}

// FromOffer converts what the store returned into the response body.
func FromOffer(o store.Offer) OfferResponse {
	resp := OfferResponse{
		ClusterID:       o.ClusterID,
		LiqoClusterID:   o.LiqoClusterID,
		ClusterType:     o.ClusterType,
		ProbeEndpoint:   o.ProbeEndpoint,
		CarbonIntensity: o.CarbonIntensity,
		CarbonForecast:  o.CarbonForecast,
		Renewable:       o.Renewable,
		TotalChunks:     o.TotalChunks,
		ReservedChunks:  o.ReservedChunks,
		AvailableChunks: o.AvailableChunks(),
		LastSeen:        o.LastSeen.UTC(),
		Fresh:           o.Fresh,
		Extra:           o.Extra,
	}
	t := o.Topology
	if t.Zone != "" || t.Region != "" || t.City != "" || t.HasLocation {
		tw := &TopologyWire{Zone: t.Zone, Region: t.Region, City: t.City}
		if t.HasLocation {
			lat, lon := t.Latitude, t.Longitude
			tw.Latitude, tw.Longitude = &lat, &lon
		}
		resp.Topology = tw
	}
	resp.Resources = make([]ResourceWire, 0, len(o.Resources))
	for _, res := range o.Resources {
		resp.Resources = append(resp.Resources, ResourceWire{
			Name:         res.Name,
			Allocatable:  res.Allocatable,
			UnitPrice:    res.UnitPrice,
			ScalePercent: res.ScalePercent,
			FixedCap:     res.FixedCap,
		})
	}
	return resp
}
