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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	brokerv1alpha1 "github.com/netgroup-polito/federation-autoscaler/api/broker/v1alpha1"
)

// CRStore keeps the catalog in ClusterAdvertisement custom resources, which
// is where it lives today. It exists so the switch to the Store interface
// changes no behaviour: the storage moves only once a different
// implementation is selected.
//
// This file is the only place in the package that knows about Kubernetes.
type CRStore struct {
	client    client.Client
	namespace string
	freshFor  time.Duration

	now func() time.Time
}

// NewCRStore returns a store backed by ClusterAdvertisements in namespace.
func NewCRStore(c client.Client, namespace string, freshFor time.Duration) *CRStore {
	return &CRStore{client: c, namespace: namespace, freshFor: freshFor, now: time.Now}
}

var _ OfferStore = (*CRStore)(nil)

// UpsertOffer writes the spec, then refreshes the status.
//
// It mirrors what upsertClusterAdvertisement and
// touchClusterAdvertisementStatus do today, including the part that matters
// most: Status.ReservedChunks is read but never written, because reservation
// accounting is owned by the reservation path.
func (s *CRStore) UpsertOffer(ctx context.Context, o Offer) (Offer, error) {
	if o.ClusterID == "" {
		return Offer{}, fmt.Errorf("store: ClusterID is required")
	}

	cadv := &brokerv1alpha1.ClusterAdvertisement{
		ObjectMeta: metav1.ObjectMeta{Name: o.ClusterID, Namespace: s.namespace},
	}

	spec, err := specFromOffer(o)
	if err != nil {
		return Offer{}, err
	}

	if _, err := controllerutil.CreateOrUpdate(ctx, s.client, cadv, func() error {
		cadv.Spec = spec
		return nil
	}); err != nil {
		return Offer{}, fmt.Errorf("store: upsert %s/%s: %w", s.namespace, o.ClusterID, err)
	}

	now := metav1.NewTime(s.now())
	patch := cadv.DeepCopy()
	patch.Status.ObservedGeneration = cadv.Generation
	patch.Status.LastSeen = &now
	patch.Status.Available = true
	patch.Status.TotalChunks = o.TotalChunks

	// ReservedChunks is deliberately left as it was read back.
	if avail := o.TotalChunks - patch.Status.ReservedChunks; avail > 0 {
		patch.Status.AvailableChunks = avail
	} else {
		patch.Status.AvailableChunks = 0
	}

	if err := s.client.Status().Update(ctx, patch); err != nil {
		return Offer{}, fmt.Errorf("store: status update %s/%s: %w", s.namespace, o.ClusterID, err)
	}

	return s.offerFromCR(patch), nil
}

// GetOffer returns one advertisement, fresh or not.
func (s *CRStore) GetOffer(ctx context.Context, clusterID string) (Offer, error) {
	var cadv brokerv1alpha1.ClusterAdvertisement
	key := types.NamespacedName{Name: clusterID, Namespace: s.namespace}

	if err := s.client.Get(ctx, key, &cadv); err != nil {
		if apierrors.IsNotFound(err) {
			return Offer{}, ErrNotFound
		}
		return Offer{}, fmt.Errorf("store: get %s/%s: %w", s.namespace, clusterID, err)
	}
	return s.offerFromCR(&cadv), nil
}

// ListOffers lists the advertisements in the namespace and filters in Go.
func (s *CRStore) ListOffers(ctx context.Context, f OfferFilter) ([]Offer, error) {
	var list brokerv1alpha1.ClusterAdvertisementList
	if err := s.client.List(ctx, &list, client.InNamespace(s.namespace)); err != nil {
		return nil, fmt.Errorf("store: list in %s: %w", s.namespace, err)
	}

	out := make([]Offer, 0, len(list.Items))
	for i := range list.Items {
		o := s.offerFromCR(&list.Items[i])
		if f.OnlyFresh && !o.Fresh {
			continue
		}
		if f.ClusterType != "" && o.ClusterType != f.ClusterType {
			continue
		}
		out = append(out, o)
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// conversion
// -----------------------------------------------------------------------------

// specFromOffer builds the custom resource's spec from a domain Offer. The
// four per-resource maps the CR keeps separately are rebuilt here from the
// single Resources slice the domain type uses.
func specFromOffer(o Offer) (brokerv1alpha1.ClusterAdvertisementSpec, error) {
	allocatable := corev1.ResourceList{}
	var prices, fixed corev1.ResourceList
	var pct map[corev1.ResourceName]int32

	for _, r := range o.Resources {
		name := corev1.ResourceName(r.Name)

		q, err := resource.ParseQuantity(r.Allocatable)
		if err != nil {
			return brokerv1alpha1.ClusterAdvertisementSpec{},
				fmt.Errorf("store: resource %q: allocatable %q: %w", r.Name, r.Allocatable, err)
		}
		allocatable[name] = q

		if r.UnitPrice != nil {
			p, err := resource.ParseQuantity(*r.UnitPrice)
			if err != nil {
				return brokerv1alpha1.ClusterAdvertisementSpec{},
					fmt.Errorf("store: resource %q: unit price %q: %w", r.Name, *r.UnitPrice, err)
			}
			if prices == nil {
				prices = corev1.ResourceList{}
			}
			prices[name] = p
		}
		if r.FixedCap != nil {
			c, err := resource.ParseQuantity(*r.FixedCap)
			if err != nil {
				return brokerv1alpha1.ClusterAdvertisementSpec{},
					fmt.Errorf("store: resource %q: fixed cap %q: %w", r.Name, *r.FixedCap, err)
			}
			if fixed == nil {
				fixed = corev1.ResourceList{}
			}
			fixed[name] = c
		}
		if r.ScalePercent != nil {
			if pct == nil {
				pct = map[corev1.ResourceName]int32{}
			}
			pct[name] = *r.ScalePercent
		}
	}

	return brokerv1alpha1.ClusterAdvertisementSpec{
		ClusterID:            o.ClusterID,
		LiqoClusterID:        o.LiqoClusterID,
		ClusterType:          brokerv1alpha1.ChunkType(o.ClusterType),
		Resources:            brokerv1alpha1.AdvertisedResources{Allocatable: allocatable},
		Topology:             topologyToCR(o.Topology),
		UnitPrices:           prices,
		CarbonIntensity:      o.CarbonIntensity,
		CarbonForecast:       o.CarbonForecast,
		CapacityScalePercent: pct,
		CapacityFixed:        fixed,
		Renewable:            o.Renewable,
		ProbeEndpoint:        o.ProbeEndpoint,
	}, nil
}

// offerFromCR builds a domain Offer from the custom resource.
//
// Fresh is computed from LastSeen rather than read from Status.Available, so
// this store answers the same way the others do and does not depend on the
// advertisement controller having run.
func (s *CRStore) offerFromCR(cadv *brokerv1alpha1.ClusterAdvertisement) Offer {
	o := Offer{
		ClusterID:       cadv.Spec.ClusterID,
		LiqoClusterID:   cadv.Spec.LiqoClusterID,
		ClusterType:     string(cadv.Spec.ClusterType),
		Topology:        topologyFromCR(cadv.Spec.Topology),
		Resources:       resourcesFromCR(cadv.Spec),
		ProbeEndpoint:   cadv.Spec.ProbeEndpoint,
		CarbonIntensity: cadv.Spec.CarbonIntensity,
		Renewable:       cadv.Spec.Renewable,
		TotalChunks:     cadv.Status.TotalChunks,
		ReservedChunks:  cadv.Status.ReservedChunks,
	}

	if cadv.Spec.CarbonForecast != nil {
		o.CarbonForecast = append([]float64(nil), cadv.Spec.CarbonForecast...)
	}
	if cadv.Status.LastSeen != nil {
		o.LastSeen = cadv.Status.LastSeen.Time
		o.Fresh = s.now().Sub(o.LastSeen) <= s.freshFor
	}
	return o
}

// resourcesFromCR folds the CR's four per-resource maps back into one slice,
// keyed by the names present in Allocatable.
func resourcesFromCR(spec brokerv1alpha1.ClusterAdvertisementSpec) []Resource {
	if len(spec.Resources.Allocatable) == 0 {
		return nil
	}

	out := make([]Resource, 0, len(spec.Resources.Allocatable))
	for name, q := range spec.Resources.Allocatable {
		r := Resource{Name: string(name), Allocatable: q.String()}

		if p, ok := spec.UnitPrices[name]; ok {
			v := p.String()
			r.UnitPrice = &v
		}
		if c, ok := spec.CapacityFixed[name]; ok {
			v := c.String()
			r.FixedCap = &v
		}
		if pct, ok := spec.CapacityScalePercent[name]; ok {
			v := pct
			r.ScalePercent = &v
		}
		out = append(out, r)
	}
	return out
}

func topologyToCR(t Topology) *brokerv1alpha1.Topology {
	if t == (Topology{}) {
		return nil
	}
	return &brokerv1alpha1.Topology{
		Zone:      t.Zone,
		Region:    t.Region,
		City:      t.City,
		Latitude:  t.Latitude,
		Longitude: t.Longitude,
	}
}

// topologyFromCR mirrors how the placement code already reads location: a
// provider has one only when the topology is present and the coordinates are
// not both zero, since 0,0 cannot be told apart from "unset" in the CR.
func topologyFromCR(t *brokerv1alpha1.Topology) Topology {
	if t == nil {
		return Topology{}
	}
	return Topology{
		Zone:        t.Zone,
		Region:      t.Region,
		City:        t.City,
		Latitude:    t.Latitude,
		Longitude:   t.Longitude,
		HasLocation: t.Latitude != 0 || t.Longitude != 0,
	}
}
