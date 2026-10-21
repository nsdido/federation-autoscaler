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

// Package store holds the Broker's storage behind an interface, so the same
// code can run on top of Kubernetes custom resources, a SQL database or a
// key-value store such as Redis.
//
// The interface is written at the level of business operations rather than
// generic read and write. That is what makes several backends possible: a
// generic "read, write, transaction" interface could not be implemented
// honestly by all of them, because Kubernetes has no multi-object
// transactions and Redis has no queries.
package store

import (
	"context"
	"errors"
	"time"
)

// Offer is one entry in the catalog: a provider's standing offer of capacity.
//
// It is a plain Go type on purpose. Nothing here depends on Kubernetes, so
// the callers of this package do not depend on Kubernetes either.
type Offer struct {
	ClusterID     string
	LiqoClusterID string
	ClusterType   string // "standard" or "gpu"

	// Topology carries its own HasLocation flag, so it is a value: an empty
	// Topology means nothing is known about where this provider is.
	Topology  Topology
	Resources []Resource

	ProbeEndpoint string

	// CarbonIntensity is nil when the provider reported no value.
	CarbonIntensity *float64
	CarbonForecast  []float64
	Renewable       bool

	// TotalChunks is how many chunks this offer is worth, as computed by the
	// chunk sizer before the offer reaches the store.
	TotalChunks int32

	// ReservedChunks is how many of those chunks are currently held by
	// reservations. UpsertOffer never writes it; see the method comment.
	ReservedChunks int32

	// LastSeen is when the provider last advertised. It is an output only:
	// UpsertOffer stamps it itself and ignores whatever the caller passed,
	// so a stale timestamp can never be written in.
	LastSeen time.Time

	// Fresh is set by the store when it returns an Offer: true while the
	// provider is still within the store's freshness window. It is never an
	// input, and it is not stored as a field; each backend decides how to
	// answer it.
	Fresh bool
}

// AvailableChunks is how many chunks can still be reserved. It is computed,
// never stored, and never negative.
func (o Offer) AvailableChunks() int32 {
	if n := o.TotalChunks - o.ReservedChunks; n > 0 {
		return n
	}
	return 0
}

// Topology is where a provider is.
type Topology struct {
	Zone   string
	Region string
	City   string

	Latitude  float64
	Longitude float64

	// HasLocation distinguishes "at 0,0" from "unknown", which the
	// coordinates alone cannot express.
	HasLocation bool
}

// Resource is one resource a provider offers, with the limits and the price
// it set for it.
type Resource struct {
	Name string // "cpu", "memory", "nvidia.com/gpu"

	// Allocatable is the quantity as the provider reported it, for example
	// "8" or "16Gi". It is kept as text so every backend can store it
	// unchanged; parse it in Go where arithmetic is needed.
	Allocatable string

	// UnitPrice is text for the same reason as Allocatable: the advertisement
	// carries it as a Kubernetes quantity, so keeping the text round-trips it
	// exactly instead of rounding through a float.
	UnitPrice    *string
	ScalePercent *int32
	FixedCap     *string
}

// OfferFilter narrows what ListOffers returns.
//
// It is deliberately a small closed set of options and not a query language.
// Every field here has to be answerable by an index that a key-value backend
// can maintain by itself; anything that would need a free-form search does
// not belong in this struct.
type OfferFilter struct {
	// OnlyFresh keeps only providers still within the freshness window.
	//
	// Note it is a flag and not a timestamp. A timestamp ("seen since T")
	// would work in SQL but is impossible in Redis, where a key either
	// exists or does not and cannot be searched by time. So the window
	// itself belongs inside the implementation, chosen when the store is
	// built: SQL compares LastSeen, Redis lets the key expire.
	OnlyFresh bool

	// ClusterType keeps only offers of this type. Empty means any.
	ClusterType string
}

// OfferStore is the catalog: the list of registered offers that consumers,
// and later FiberCop, read.
type OfferStore interface {
	// UpsertOffer records a provider's current offer and renews its
	// freshness. It is called on every advertisement, so roughly every
	// thirty seconds per provider.
	//
	// It must never write ReservedChunks. That counter belongs to the
	// reservation path, and overwriting it on every advertisement would
	// reset it to zero and let the same chunks be sold twice.
	//
	// It returns the stored offer, with Fresh and ReservedChunks filled in.
	UpsertOffer(ctx context.Context, o Offer) (Offer, error)

	// GetOffer returns one offer, fresh or not. It returns ErrNotFound when
	// no provider with that cluster ID has ever advertised.
	GetOffer(ctx context.Context, clusterID string) (Offer, error)

	// ListOffers returns the offers matching f, in no particular order.
	ListOffers(ctx context.Context, f OfferFilter) ([]Offer, error)
}

// ErrNotFound is returned when the requested entry does not exist.
var ErrNotFound = errors.New("store: not found")
