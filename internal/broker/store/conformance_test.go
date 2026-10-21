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

package store

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// conformanceFreshFor is the freshness window the shared specs use. It is a
// real window and the staleness spec really waits, because a key-value
// backend expires keys on its own clock and no fake clock can move it.
//
// It has to be comfortably longer than one second. A backend that stores the
// timestamp in a serialised form keeps it at second granularity — the
// Kubernetes API encodes metav1.Time as RFC 3339 with no fraction, and Redis
// key expiry is also whole seconds — so a just-written entry can read back as
// up to a second old. With a sub-second window every backend but the in-memory
// one would report a brand new offer as already stale. The real window in the
// broker is ninety seconds, so this is the same shape, only smaller.
const conformanceFreshFor = 2 * time.Second

// staleWait is how long the specs wait to push an entry past the window: the
// window itself, plus the one second a truncated timestamp can hide, plus a
// margin.
const staleWait = conformanceFreshFor + 1500*time.Millisecond

// RunOfferStoreConformance defines the behaviour every OfferStore must have,
// whatever it stores the catalog in. Call it once per implementation; if a
// new backend passes these specs, swapping to it is safe.
//
// newStore must return a fresh, empty store using the given freshness window.
func RunOfferStoreConformance(newStore func(freshFor time.Duration) OfferStore) {
	var (
		ctx context.Context
		s   OfferStore
	)

	BeforeEach(func() {
		ctx = context.Background()
		s = newStore(conformanceFreshFor)
	})

	It("returns what was stored", func() {
		_, err := s.UpsertOffer(ctx, sampleOffer("cluster-a"))
		Expect(err).NotTo(HaveOccurred())

		got, err := s.GetOffer(ctx, "cluster-a")
		Expect(err).NotTo(HaveOccurred())
		Expect(got.ClusterID).To(Equal("cluster-a"))
		Expect(got.ClusterType).To(Equal("standard"))
		Expect(got.TotalChunks).To(Equal(int32(4)))
		Expect(got.Topology.City).To(Equal("Turin"))
		Expect(got.Resources).To(HaveLen(2))
		Expect(got.LastSeen).NotTo(BeZero())
	})

	It("returns ErrNotFound for a provider that never advertised", func() {
		_, err := s.GetOffer(ctx, "nobody")
		Expect(err).To(MatchError(ErrNotFound))
	})

	It("overwrites an existing offer rather than duplicating it", func() {
		_, err := s.UpsertOffer(ctx, sampleOffer("cluster-a"))
		Expect(err).NotTo(HaveOccurred())

		second := sampleOffer("cluster-a")
		second.TotalChunks = 9
		_, err = s.UpsertOffer(ctx, second)
		Expect(err).NotTo(HaveOccurred())

		all, err := s.ListOffers(ctx, OfferFilter{})
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(1))
		Expect(all[0].TotalChunks).To(Equal(int32(9)))
	})

	It("ignores ReservedChunks sent by the caller", func() {
		// The advertisement knows nothing about reservations. If an upsert
		// could write this counter, a provider's own heartbeat would free
		// chunks that are still held, and they would be sold twice.
		o := sampleOffer("cluster-a")
		o.ReservedChunks = 5

		stored, err := s.UpsertOffer(ctx, o)
		Expect(err).NotTo(HaveOccurred())
		Expect(stored.ReservedChunks).To(BeZero())

		got, err := s.GetOffer(ctx, "cluster-a")
		Expect(err).NotTo(HaveOccurred())
		Expect(got.ReservedChunks).To(BeZero())
	})

	It("computes AvailableChunks and never goes negative", func() {
		o := Offer{ClusterID: "x", TotalChunks: 4, ReservedChunks: 1}
		Expect(o.AvailableChunks()).To(Equal(int32(3)))

		o.ReservedChunks = 7
		Expect(o.AvailableChunks()).To(BeZero())
	})

	It("marks a new offer fresh", func() {
		stored, err := s.UpsertOffer(ctx, sampleOffer("cluster-a"))
		Expect(err).NotTo(HaveOccurred())
		Expect(stored.Fresh).To(BeTrue())
	})

	It("stops reporting an offer as fresh once the window passes", func() {
		_, err := s.UpsertOffer(ctx, sampleOffer("cluster-a"))
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(staleWait)

		// GetOffer still finds it: going stale is not the same as being
		// deleted, and the entry stays in the catalog.
		got, err := s.GetOffer(ctx, "cluster-a")
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Fresh).To(BeFalse())

		fresh, err := s.ListOffers(ctx, OfferFilter{OnlyFresh: true})
		Expect(err).NotTo(HaveOccurred())
		Expect(fresh).To(BeEmpty())

		all, err := s.ListOffers(ctx, OfferFilter{})
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(1))
	})

	It("becomes fresh again when the provider advertises once more", func() {
		_, err := s.UpsertOffer(ctx, sampleOffer("cluster-a"))
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(staleWait)

		stored, err := s.UpsertOffer(ctx, sampleOffer("cluster-a"))
		Expect(err).NotTo(HaveOccurred())
		Expect(stored.Fresh).To(BeTrue())

		fresh, err := s.ListOffers(ctx, OfferFilter{OnlyFresh: true})
		Expect(err).NotTo(HaveOccurred())
		Expect(fresh).To(HaveLen(1))
	})

	It("filters by cluster type", func() {
		std := sampleOffer("cluster-a")
		gpu := sampleOffer("cluster-b")
		gpu.ClusterType = "gpu"

		_, err := s.UpsertOffer(ctx, std)
		Expect(err).NotTo(HaveOccurred())
		_, err = s.UpsertOffer(ctx, gpu)
		Expect(err).NotTo(HaveOccurred())

		only, err := s.ListOffers(ctx, OfferFilter{ClusterType: "gpu"})
		Expect(err).NotTo(HaveOccurred())
		Expect(only).To(HaveLen(1))
		Expect(only[0].ClusterID).To(Equal("cluster-b"))

		all, err := s.ListOffers(ctx, OfferFilter{})
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(2))
	})

	It("returns an empty list, not an error, when the catalog is empty", func() {
		all, err := s.ListOffers(ctx, OfferFilter{})
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(BeEmpty())
	})

	It("hands back a copy the caller cannot use to reach into the store", func() {
		// A real backend serialises, so a caller can never hold a reference
		// to what it stored. Any implementation has to behave that way too,
		// or code that works on one backend corrupts another.
		_, err := s.UpsertOffer(ctx, sampleOffer("cluster-a"))
		Expect(err).NotTo(HaveOccurred())

		got, err := s.GetOffer(ctx, "cluster-a")
		Expect(err).NotTo(HaveOccurred())
		got.Resources[0].Name = "tampered"
		*got.CarbonIntensity = 999

		again, err := s.GetOffer(ctx, "cluster-a")
		Expect(err).NotTo(HaveOccurred())
		Expect(again.Resources[0].Name).To(Equal("cpu"))
		Expect(*again.CarbonIntensity).To(BeNumerically("==", 120))
	})

	// Pending until the reservation side of the Store exists. The rule is
	// that an upsert carries the previous ReservedChunks over, which cannot
	// be set up through the catalog methods alone: nothing here can put a
	// non-zero value in the counter.
	PIt("carries the previous ReservedChunks over on upsert", func() {})
}

// sampleOffer is a complete, valid offer used across the specs.
func sampleOffer(clusterID string) Offer {
	carbon := 120.0
	price := "0.05"
	pct := int32(80)

	return Offer{
		ClusterID:     clusterID,
		LiqoClusterID: "liqo-" + clusterID,
		ClusterType:   "standard",
		Topology: Topology{
			Zone:        "eu-west",
			Region:      "piemonte",
			City:        "Turin",
			Latitude:    45.07,
			Longitude:   7.69,
			HasLocation: true,
		},
		Resources: []Resource{
			{Name: "cpu", Allocatable: "8", UnitPrice: &price, ScalePercent: &pct},
			{Name: "memory", Allocatable: "16Gi"},
		},
		ProbeEndpoint:   "10.0.0.1:9999",
		CarbonIntensity: &carbon,
		CarbonForecast:  []float64{100, 110, 120},
		Renewable:       true,
		TotalChunks:     4,
	}
}
