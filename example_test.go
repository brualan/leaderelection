package leaderelection_test

import (
	"context"
	"fmt"
	"time"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/memstore"
)

// The common shape: a callback that runs for the whole term and stops when its
// context is cancelled.
func Example() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})

	elector, err := leaderelection.New(leaderelection.Config{
		Store: memstore.New(),
		Name:  "invoice-reconciler",
		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(ctx context.Context, fence int64) {
				fmt.Println("leading with fence", fence)
				close(done)
				<-ctx.Done()
			},
		},
	})
	if err != nil {
		panic(err)
	}

	go func() {
		if err := elector.Run(ctx); err != nil {
			panic(err)
		}
	}()
	<-done

	// Output:
	// leading with fence 1
}

// A fence token is what protects a system that cannot take part in the
// election. Stamp every write with it, and have the receiving side refuse
// anything older than the highest token it has seen.
func ExampleElector_Fence() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	elector, err := leaderelection.New(leaderelection.Config{
		Store: memstore.New(),
		Name:  "shipping",
	})
	if err != nil {
		panic(err)
	}
	go func() {
		if err := elector.Run(ctx); err != nil {
			panic(err)
		}
	}()

	for {
		if fence, ok := elector.Fence(); ok {
			fmt.Println("would write with fence", fence)
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Output:
	// would write with fence 1
}

// Followers can read what the leader published, which across clusters is often
// the only channel that exists.
func ExampleElector_Status() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := memstore.New()
	leader, err := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "gateway",
		Identity: "eu-west",
		Payload:  []byte("https://leader.eu-west.internal:8443"),
	})
	if err != nil {
		panic(err)
	}
	go func() {
		if err := leader.Run(ctx); err != nil {
			panic(err)
		}
	}()

	// Wait for the term to start before the follower joins, so that the
	// follower's first read already sees a leader rather than an empty
	// election it would have to poll again for.
	for !leader.IsLeader() {
		time.Sleep(time.Millisecond)
	}

	follower, err := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "gateway",
		Identity: "us-east",
	})
	if err != nil {
		panic(err)
	}
	go func() {
		if err := follower.Run(ctx); err != nil {
			panic(err)
		}
	}()

	for {
		if status := follower.Status(); status.Leader == "eu-west" {
			fmt.Printf("leader %s reachable at %s\n", status.Leader, status.LeaderPayload)
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Output:
	// leader eu-west reachable at https://leader.eu-west.internal:8443
}

// New refuses a configuration whose timings leave a leader no room to lose a
// single renewal, because such a configuration flaps under exactly the
// conditions the election exists to survive.
func ExampleConfig_Validate() {
	err := leaderelection.Config{
		Store:            memstore.New(),
		Name:             "too-tight",
		Identity:         "a",
		LeaseDuration:    10 * time.Second,
		RenewInterval:    5 * time.Second,
		RetryPeriod:      time.Second,
		OperationTimeout: 3 * time.Second,
		SafetyMargin:     time.Second,
	}.Validate()

	fmt.Println(err)

	// Output:
	// leaderelection: invalid config: RenewInterval: a lost renewal needs RenewInterval+2*OperationTimeout+retry (12.25s) but only LeaseDuration-SafetyMargin (9s) is available
}
