package leaderelection_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestCronSchedulerStopsWhenLeadershipIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { defer close(done); runCronTasks(ctx, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cron scheduler deadlocked on leadership cancellation without an active daily task")
	}
}

func TestKafkaConsumerStopsAndJoinsProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); consumeKafka(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), 1) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Kafka consumer did not finish after leadership cancellation")
	}
}
