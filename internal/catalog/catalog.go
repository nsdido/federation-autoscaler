// Package catalog is the shared offer catalog used by the provider agent,
// the consumer side and the broker.
//
// The catalog is deliberately "as stupid as possible": it stores offers and
// returns them. It does not rank, choose, count reservations or compute
// anything. There is no controller and no polling loop next to it.
package catalog

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when an offer is missing or has expired.
var ErrNotFound = errors.New("catalog: offer not found")

const (
	// OfferTTL is how long an offer lives without being refreshed.
	OfferTTL = 90 * time.Second
	// RefreshEvery is how often a writer must refresh its offers.
	RefreshEvery = 30 * time.Second
)

// Writer is used by providers (and, at first, by the broker).
type Writer interface {
	// UpsertOffer stores the offer with OfferTTL.
	UpsertOffer(ctx context.Context, o Offer) error
	// DeleteOffer removes the offer of one (provider, site).
	DeleteOffer(ctx context.Context, providerID, siteID string) error
}

// Reader is used by the consumer side and the broker.
type Reader interface {
	// GetOffer returns ErrNotFound if the offer is missing or expired.
	GetOffer(ctx context.Context, providerID, siteID string) (Offer, error)
	// ListOffers returns all live offers, in no particular order.
	ListOffers(ctx context.Context) ([]Offer, error)
}
