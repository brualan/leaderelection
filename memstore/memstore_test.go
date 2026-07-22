package memstore_test

import (
	"testing"

	"github.com/brualan/leaderelection/memstore"
	"github.com/brualan/leaderelection/storetest"
)

// The in-memory store is the reference implementation. Running the acceptance
// suite against it validates the suite as much as the store: a scenario that
// cannot pass here describes a behaviour no implementation could provide.
func TestAcceptance(t *testing.T) {
	storetest.Run(t, storetest.Suite{Store: memstore.New()})
}
