//go:build chaos

package tests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/brualan/leaderelection/tests/harness"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// More election participants than pool slots is only contention: short queries
// can still all succeed. Pin every server connection in a separate transaction
// to force actual starvation, followed by recovery without restarting clients.
func TestPoolerServerConnectionsAreExhausted(t *testing.T) {
	for _, tc := range []struct {
		backend *harness.Backend
		driver  string
		slots   int
	}{{harness.PostgresBackend, "pgx", 5}, {harness.MySQLBackend, "mysql", 50}} {
		t.Run(tc.backend.Name, func(t *testing.T) {
			c := newClusterOn(t, tc.backend, tc.backend.Pooled, 3)
			first := c.AwaitLeader(t, 20*time.Second)
			fence, ok := first.Elector.Fence()
			if !ok {
				t.Fatal("initial leader has no fence")
			}
			proxy := harness.NewToxiproxy(t, harness.ToxiproxyURL()).CreateProxy(t, c.Election+"-blockers", ports(t, 1), tc.backend.Pooled)
			db, err := sql.Open(tc.driver, fmt.Sprintf(tc.backend.DSNTemplate, proxy.Listen))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(tc.slots)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			var held []*sql.Tx
			release := func() {
				for _, tx := range held {
					_ = tx.Rollback()
				}
				held = nil
			}
			defer release()
			for range tc.slots {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatalf("pin server connection %d: %v", len(held)+1, err)
				}
				held = append(held, tx)
				// An InnoDB table read starts a real transaction even in ProxySQL's
				// multiplexing mode, unlike a constant SELECT which may not pin it.
				var count int64
				if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM leader_election").Scan(&count); err != nil {
					t.Fatal(err)
				}
			}
			c.AwaitNoLeader(t, 2*harness.LeaseDuration)
			deadline := time.Now().Add(harness.LeaseDuration)
			for time.Now().Before(deadline) {
				if c.Leader(t) != nil {
					t.Fatal("participant led without a free backend connection")
				}
				time.Sleep(5 * time.Millisecond)
			}
			release()
			recovered := c.AwaitLeader(t, 20*time.Second)
			next, ok := recovered.Elector.Fence()
			if !ok || next <= fence {
				t.Fatal("recovery did not start a fresh term")
			}
			c.Sample(t, harness.LeaseDuration)
			c.Observer.Check(t)
		})
	}
}
