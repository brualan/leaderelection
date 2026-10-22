package leaderelection_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/memstore"
)

// ExampleCronService demonstrates a cron service using leader election.
// Only the leader runs periodic tasks to prevent duplicate work across replicas.
func Example_cronService() {
	// Shared database store for election (in production, use Postgres or MySQL)
	store := memstore.New()
	logger := slog.Default()

	// Configure the elector for the cron service
	elector, err := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "cron-service",           // All replicas use the same name
		Identity: "replica-1",              // Unique per instance (hostname, pod name, etc)
		Payload:  []byte("cron-service-1"), // Optional: advertise service location

		// Timing: lease lasts 15s, renew every 5s, retry failed renewals every 3s
		LeaseDuration: 15 * time.Second,
		RenewInterval: 5 * time.Second,
		RetryPeriod:   3 * time.Second,

		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(ctx context.Context, fence int64) {
				logger.Info("became leader", "fence", fence)
				runCronTasks(ctx, logger)
				logger.Info("stopped running cron tasks")
			},
			OnStoppedLeading: func() {
				logger.Info("lost leadership")
			},
			OnNewLeader: func(identity string, fence int64) {
				logger.Info("new leader elected", "identity", identity, "fence", fence)
			},
		},
	})
	if err != nil {
		panic(err)
	}

	// Run the election in the background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := elector.Run(ctx); err != nil {
			panic(err)
		}
	}()

	// Wait for leadership to be established
	for !elector.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	// Simulate service running for a bit
	time.Sleep(2 * time.Second)
}

// runCronTasks runs the periodic tasks while holding leadership.
// When ctx is cancelled (leadership lost), all tasks stop gracefully.
func runCronTasks(ctx context.Context, logger *slog.Logger) {
	// Track daily work so shutdown waits for its cancellation to finish.
	var lastDailyTask time.Time
	dailyTaskCtx, cancelDaily := context.WithCancel(ctx)
	defer cancelDaily()
	var dailyTasks sync.WaitGroup
	defer dailyTasks.Wait()

	// Ticker for 15-minute interval tasks
	ticker15 := time.NewTicker(15 * time.Minute)
	defer ticker15.Stop()

	// Ticker for checking if it's time to run the daily task
	tickerDaily := time.NewTicker(1 * time.Minute)
	defer tickerDaily.Stop()

	for {
		select {
		case <-ctx.Done():
			// Lost leadership: stop scheduling and cancel in-flight work.
			logger.Info("stopping cron scheduler")
			// The deferred Wait joins actual work; waiting for a context
			// cancelled only by a defer here would deadlock.
			return

		case <-ticker15.C:
			// Run 15-minute task (short duration, safe to run within the loop)
			logger.Info("running 15-minute task")
			if err := runQuickTask15Min(ctx); err != nil {
				logger.Error("15-minute task failed", "error", err)
			}

		case <-tickerDaily.C:
			// Check if it's time for the daily task (e.g., at 2 AM)
			now := time.Now()
			dayKey := now.Format("2006-01-02")
			lastKey := lastDailyTask.Format("2006-01-02")

			if dayKey != lastKey && now.Hour() == 2 {
				lastDailyTask = now
				// Run daily task in a separate goroutine so it doesn't block the scheduler
				dailyTasks.Add(1)
				go func() {
					defer dailyTasks.Done()
					runDailyTask(ctx, dailyTaskCtx, logger)
				}()
			}
		}
	}
}

// runQuickTask15Min runs a short, predictable task.
// It respects the context and exits quickly if leadership is lost.
func runQuickTask15Min(ctx context.Context) error {
	// Example: fetch records and process them
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Do work, checking context occasionally for long operations
	for i := 0; i < 10; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Process a batch
			time.Sleep(10 * time.Millisecond)
		}
	}

	return nil
}

// runDailyTask runs a long-running task with unknown duration.
// The task context inherits leadership cancellation; shutdown joins the task.
func runDailyTask(schedCtx, taskCtx context.Context, logger *slog.Logger) {
	logger.Info("starting daily task")

	// Derive cancellation for this individual task.
	ctx, cancel := context.WithCancel(taskCtx)
	defer cancel()

	// Run the task in parallel with checking if we lost leadership
	taskDone := make(chan error, 1)
	go func() {
		taskDone <- performLongRunningWork(ctx, logger)
	}()

	select {
	case err := <-taskDone:
		if err != nil {
			logger.Error("daily task failed", "error", err)
		} else {
			logger.Info("daily task completed successfully")
		}
	case <-schedCtx.Done():
		// Leadership lost while task was running
		logger.Info("lost leadership while daily task was running, waiting for cancellation",
			"reason", schedCtx.Err())
		// Wait for the cancelled task to stop before ending the callback.
		if err := <-taskDone; err != nil {
			logger.Error("daily task failed after losing leadership", "error", err)
		}
	}
}

// performLongRunningWork simulates a task with unknown duration.
// In reality, this could be data reconciliation, report generation, etc.
func performLongRunningWork(ctx context.Context, logger *slog.Logger) error {
	// Simulate work that takes a variable amount of time
	steps := []struct {
		name     string
		duration time.Duration
	}{
		{"fetching data", 500 * time.Millisecond},
		{"processing batch 1", 700 * time.Millisecond},
		{"processing batch 2", 600 * time.Millisecond},
		{"generating report", 800 * time.Millisecond},
		{"uploading results", 400 * time.Millisecond},
	}

	for _, step := range steps {
		select {
		case <-ctx.Done():
			logger.Warn("daily task interrupted", "step", step.name)
			return ctx.Err()
		default:
		}

		logger.Info("daily task step", "step", step.name)
		time.Sleep(step.duration)
	}

	return nil
}

// ExampleFollowerMonitoring demonstrates how non-leader replicas can monitor
// the leader's status and potentially route work to it.
func Example_followerMonitoring() {
	store := memstore.New()
	logger := slog.Default()

	// Leader instance
	leader, err := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "cron-service",
		Identity: "replica-leader",
		Payload:  []byte("https://leader-replica.internal:8080"),
	})
	if err != nil {
		panic(err)
	}

	// Follower instance
	follower, err := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "cron-service",
		Identity: "replica-follower",
		Callbacks: leaderelection.Callbacks{
			OnNewLeader: func(identity string, fence int64) {
				logger.Info("follower detected new leader", "identity", identity)
			},
		},
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	followerDone := make(chan struct{})
	defer func() { cancel(); <-leaderDone; <-followerDone }()

	go func() { defer close(leaderDone); _ = leader.Run(ctx) }()

	// Wait for election
	for !leader.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	go func() { defer close(followerDone); _ = follower.Run(ctx) }()
	for follower.Status().Leader != "replica-leader" {
		time.Sleep(time.Millisecond)
	}

	// Follower can query the leader's status
	status := follower.Status()
	fmt.Printf("Follower knows leader: %s at %s\n", status.Leader, status.LeaderPayload)

	// Output:
	// Follower knows leader: replica-leader at https://leader-replica.internal:8080
}

// ExampleFencing demonstrates using fence tokens to protect external systems.
// Even if this process is stalled, it cannot send stale writes that would
// be accepted by a new leader's term.
func Example_fencing() {
	store := memstore.New()
	started := make(chan struct{})

	elector, err := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "data-writer",
		Identity: "writer-1",
		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(ctx context.Context, fence int64) {
				// Every write includes the fence token
				// External systems (S3, database, queue) reject writes with stale fences
				fmt.Printf("Leader write with fence %d\n", fence)
				close(started)
				<-ctx.Done()
			},
		},
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	defer func() { cancel(); <-done }()
	go func() { defer close(done); _ = elector.Run(ctx) }()
	<-started

	// Wait for leadership
	for !elector.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	// Can query the current fence anytime
	if fence, ok := elector.Fence(); ok {
		fmt.Printf("Current fence: %d\n", fence)
	}

	// Output:
	// Leader write with fence 1
	// Current fence: 1
}
