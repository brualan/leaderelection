package leaderelection_test

import (
	"context"
	"fmt"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/memstore"
)

// Collect a snapshot at scrape time. The output illustrates exporter mapping;
// a service can use its existing Prometheus collector without library adapters.
func ExampleElector_Metrics() {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	elector, err := leaderelection.New(leaderelection.Config{
		Store: memstore.New(), Name: "invoice-reconciler",
		Callbacks: leaderelection.Callbacks{OnStartedLeading: func(ctx context.Context, _ int64) { close(started); <-ctx.Done() }},
	})
	if err != nil {
		panic(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = elector.Run(ctx) }()
	defer func() { cancel(); <-done }()
	<-started

	m := elector.Metrics()
	leader := 0
	if m.IsLeader {
		leader = 1
	}
	// Booleans become gauges; cumulative totals become counter samples. Do
	// not repeatedly Add a cumulative total to an exporter-owned Counter.
	fmt.Printf("leader_election_is_leader %d\n", leader)
	fmt.Printf("leader_election_terms_started_total %d\n", m.TermsStartedTotal)
	fmt.Printf("leader_election_store_calls_total{operation=%q} %d\n", "acquire", m.Store.Acquire.CallsTotal)

	// Output:
	// leader_election_is_leader 1
	// leader_election_terms_started_total 1
	// leader_election_store_calls_total{operation="acquire"} 1
}
