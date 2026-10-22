package leaderelection_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/memstore"
)

// ExampleKafkaConsumer demonstrates using leader election to ensure only one
// replica consumes from a Kafka topic. This is different from the cron service:
// here we want ONE replica to own consumption for the entire service lifetime,
// not just during scheduled tasks.
func Example_kafkaConsumer() {
	store := memstore.New()
	logger := slog.Default()

	elector, err := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "kafka-consumer-group", // All replicas use same name
		Identity: "replica-1",            // Unique per instance
		Payload:  []byte("replica-1"),

		// Timing: aggressive renewal to quickly detect leader failure
		LeaseDuration: 10 * time.Second,
		RenewInterval: 2 * time.Second,
		RetryPeriod:   1 * time.Second,

		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(ctx context.Context, fence int64) {
				logger.Info("became consumer leader", "fence", fence)
				// This runs for the entire leadership term
				// It blocks until this replica loses leadership
				consumeKafka(ctx, logger, fence)
				logger.Info("stopped consuming kafka")
			},
			OnStoppedLeading: func() {
				logger.Info("lost kafka consumer leadership")
			},
		},
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run the election in background
	go func() {
		if err := elector.Run(ctx); err != nil {
			panic(err)
		}
	}()

	// Wait for this replica to become leader
	for !elector.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	// Simulate running for a bit
	time.Sleep(2 * time.Second)
}

// consumeKafka consumes messages from Kafka only while this replica is leader.
// When ctx is cancelled (leadership lost), we immediately stop consuming
// to prevent duplicate processing by the new leader.
func consumeKafka(ctx context.Context, logger *slog.Logger, fence int64) {
	// Simulated Kafka consumer
	// In production, this would be: consumer := kafkalib.NewConsumer(...)

	msgChan := make(chan string, 100)
	producerDone := make(chan struct{})
	defer func() { <-producerDone }()
	go func() {
		defer close(producerDone)
		defer close(msgChan)
		// Simulate Kafka messages arriving
		for i := 0; i < 1000; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			select {
			case msgChan <- fmt.Sprintf("message-%d", i):
			case <-ctx.Done():
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Process messages while leader
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				// Channel closed when context cancelled
				logger.Info("kafka consumer channel closed")
				return
			}

			// Process message with fence token
			// This prevents other leaders from processing the same message
			logger.Info("processing message", "msg", msg, "fence", fence)
			processKafkaMessage(ctx, msg, fence)

		case <-ctx.Done():
			// Lost leadership - stop consuming immediately
			logger.Info("lost leadership, stopping kafka consumption")
			return
		}
	}
}

func processKafkaMessage(ctx context.Context, msg string, fence int64) {
	// Store result with fence token to protect against duplicate processing
	// If new leader has higher fence, DB will reject this old fence
	_ = fmt.Sprintf("storing %s with fence %d", msg, fence)

	// Must respect context during processing
	select {
	case <-ctx.Done():
		return
	default:
		time.Sleep(10 * time.Millisecond)
	}
}

// ExampleKafkaWithRebalancing shows how to handle partition rebalancing
// when leadership changes between replicas.
func Example_kafkaWithRebalancing() {
	store := memstore.New()
	logger := slog.Default()

	type KafkaState struct {
		// Track which partitions this leader is responsible for
		ownedPartitions map[int]bool
	}

	state := &KafkaState{
		ownedPartitions: make(map[int]bool),
	}

	elector, err := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "kafka-consumer",
		Identity: "replica-1",

		LeaseDuration: 10 * time.Second,
		RenewInterval: 2 * time.Second,

		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(ctx context.Context, fence int64) {
				logger.Info("starting kafka leadership", "fence", fence)

				// When we become leader, rebalance partitions
				// In production: consumer.Subscribe(topics, rebalanceListener)
				state.ownedPartitions[0] = true
				state.ownedPartitions[1] = true
				state.ownedPartitions[2] = true

				// Consume until leadership lost
				for {
					select {
					case <-ctx.Done():
						logger.Info("rebalancing away from this leader")
						state.ownedPartitions = make(map[int]bool)
						return
					default:
						// Process messages
						time.Sleep(100 * time.Millisecond)
					}
				}
			},
		},
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := elector.Run(ctx); err != nil {
			panic(err)
		}
	}()

	for !elector.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)
}

// ExampleMultiServiceLeadership shows coordinating multiple independent
// services, each with its own leader. This is useful when you have:
// - A Kafka consumer service (needs one leader)
// - A periodic batch job (needs one leader)
// - A cache warmer (needs one leader)
// They can all run in same replicas but as independent elections.
func Example_multiServiceLeadership() {
	store := memstore.New()
	logger := slog.Default()

	// Service 1: Kafka consumer (must have leader for entire lifetime)
	kafkaElector, _ := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "kafka-consumer", // Different from other services
		Identity: "replica-1",
		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(ctx context.Context, fence int64) {
				logger.Info("kafka consumer: became leader")
				<-ctx.Done()
			},
		},
	})

	// Service 2: Batch job (can be spawned by any leader when needed)
	batchElector, _ := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "batch-processor", // Different name = different election
		Identity: "replica-1",
		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(ctx context.Context, fence int64) {
				logger.Info("batch: became leader, will run periodic jobs")
				<-ctx.Done()
			},
		},
	})

	// Service 3: Cache warmer
	cacheElector, _ := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "cache-warmer", // Another independent election
		Identity: "replica-1",
		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(ctx context.Context, fence int64) {
				logger.Info("cache: became leader")
				<-ctx.Done()
			},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// All three services compete independently in their own elections
	go func() {
		if err := kafkaElector.Run(ctx); err != nil {
			panic(err)
		}
	}()
	go func() {
		if err := batchElector.Run(ctx); err != nil {
			panic(err)
		}
	}()
	go func() {
		if err := cacheElector.Run(ctx); err != nil {
			panic(err)
		}
	}()

	// Wait for elections to complete
	for !kafkaElector.IsLeader() || !batchElector.IsLeader() || !cacheElector.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("All three services have leaders elected")

	time.Sleep(1 * time.Second)
}

// ExampleFollowerRouting shows how non-leader replicas can route requests
// to the leader (useful for coordinating work with the kafka consumer leader).
func Example_followerRouting() {
	store := memstore.New()
	observed := make(chan struct{}, 1)

	// Leader
	leader, _ := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "kafka-consumer",
		Identity: "replica-1",
		Payload:  []byte("grpc://replica-1:9090"),
	})

	// Follower
	follower, _ := leaderelection.New(leaderelection.Config{
		Store:    store,
		Name:     "kafka-consumer",
		Identity: "replica-2",
		Callbacks: leaderelection.Callbacks{
			OnNewLeader: func(identity string, fence int64) {
				// When we detect leader changed, we can route to it
				fmt.Printf("follower knows new leader: %s\n", identity)
				select {
				case observed <- struct{}{}:
				default:
				}
			},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	followerDone := make(chan struct{})
	defer func() { cancel(); <-leaderDone; <-followerDone }()

	go func() { defer close(leaderDone); _ = leader.Run(ctx) }()

	for !leader.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	go func() { defer close(followerDone); _ = follower.Run(ctx) }()
	<-observed

	// Follower can query leader's address
	status := follower.Status()
	fmt.Printf("route requests to: %s\n", status.LeaderPayload)

	// Output:
	// follower knows new leader: replica-1
	// route requests to: grpc://replica-1:9090
}
