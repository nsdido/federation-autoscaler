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
