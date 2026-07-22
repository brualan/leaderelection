package memstore_test

import (
	"testing"

	"github.com/brualan/leaderelection/memstore"
	"github.com/brualan/leaderelection/storetest"
)

// The model in storetest describes what the contract says a store must do with
// an arbitrary sequence of operations. Running it here fuzzes the reference
// implementation and the model together: a disagreement means one of them is
// wrong, and finding out which is cheaper now than when the second dialect
// arrives.
func FuzzStore(f *testing.F) {
	storetest.FuzzOperations(f, storetest.Suite{Store: memstore.New()})
}
