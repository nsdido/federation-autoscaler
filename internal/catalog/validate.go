package catalog

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidOffer is returned when an offer cannot be stored because its key
// is not valid. Use errors.Is(err, ErrInvalidOffer) to test for it.
var ErrInvalidOffer = errors.New("catalog: invalid offer")

// validateKey checks the (providerID, siteID) pair that identifies an offer.
//
// Both parts must be non-empty. They must not contain ':' because the Redis
// key is "catalog:offer:<providerId>:<siteId>": without this rule the pairs
// ("a:b", "c") and ("a", "b:c") would end up on the same key.
func validateKey(providerID, siteID string) error {
	if providerID == "" || siteID == "" {
		return fmt.Errorf("%w: providerId and siteId must not be empty", ErrInvalidOffer)
	}
	if strings.Contains(providerID, ":") || strings.Contains(siteID, ":") {
		return fmt.Errorf("%w: providerId and siteId must not contain ':'", ErrInvalidOffer)
	}
	return nil
}
