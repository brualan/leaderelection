# leaderelection

Leader election for services that share nothing but a database.

[![Go Reference](https://pkg.go.dev/badge/github.com/brualan/leaderelection.svg)](https://pkg.go.dev/github.com/brualan/leaderelection)
[![Go Report Card](https://goreportcard.com/badge/github.com/brualan/leaderelection)](https://goreportcard.com/report/github.com/brualan/leaderelection)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

```go
elector, _ := leaderelection.New(leaderelection.Config{
    Store: store,
    Name:  "invoice-reconciler",
    Callbacks: leaderelection.Callbacks{
        OnStartedLeading: func(ctx context.Context, fence int64) {
            reconcile(ctx, fence) // stop when ctx is cancelled
        },
    },
})
elector.Run(ctx)
```

---

## Why this exists

Kubernetes ships a perfectly good leader election, and you should use it — right
up until your replicas stop sharing a cluster.

Run the same service in two regions, in two clusters with no network path
between them, and `coordination.k8s.io/Lease` cannot help: neither cluster's API
server knows the other exists. The replicas still have exactly one thing in
common — the database they were already talking to. So that is where the
election happens.

This library elects one leader among processes that share a PostgreSQL or MySQL
instance and nothing else. No etcd, no ZooKeeper, no Consul, no
extra thing to operate. A table with one row per election.

It is built for the awkward version of that deployment: participants on the far
side of a WAN, clocks that disagree, a PgBouncer in the path, and a database
that occasionally stops answering.

## Guarantees

**At most one participant considers itself the leader at any instant.** That
holds through network partitions, database outages, connection pooler restarts,
and processes killed without warning.

Three choices carry that weight.

**Every timestamp comes from the database.** Participants in different clusters
have unrelated clocks, so a lease expiry expressed as an absolute time would
mean different things to different participants. Each statement returns both the
expiry *and* the server's current time, and only their difference — a duration —
is ever used. Your clock can be three days off; nothing changes.

**That duration is measured against the local monotonic clock, from the moment
the request was sent.** Not from when the reply arrived: the database started
counting somewhere in between, and anchoring at the earlier bound keeps the
local deadline strictly *earlier* than the real one.

**A watchdog enforces that deadline independently of the renewal loop.** This is
what makes an unreachable database safe rather than merely inconvenient. A
renewal that hangs forever cannot delay the step-down, because nothing about
stepping down waits on the database. If a process cannot prove it still holds
the lease, it stops acting as though it does — and it does so before the lease
could possibly be handed to anyone else.

Uncertainty always resolves to *not leader*.

## What it does not guarantee

**It is not a consensus protocol.** The database is the single source of truth
and the single point of failure. If it is down, nobody leads. That is the
correct outcome, and it is the trade you are making by not running etcd.

**Liveness is bounded by the lease.** A leader that dies without releasing takes
up to `LeaseDuration` to be replaced. Measured on the test suite: **~3.09s with a
3s lease** after an unannounced crash, **~0.43s** after a clean shutdown, because
a graceful exit releases the lease instead of letting it expire.

**Stepping down is not the same as having stopped.** The library cancels your
context before anyone else can lead, but it cannot unsend a write already in
flight. For that you need fencing tokens — see below.

**The database's own clock has to move forwards.** Participants' clocks may be
days out and may run at different rates; the database's may not jump ahead. A
server clock that steps forward retires a live lease early, and no participant
reading that clock can tell it happened — the incumbent's deadline still says
it holds what the database has already given away. Steps backwards are safe:
they make every lease look longer to everyone at once, and the elector refuses
to believe a lease longer than the one it asked for.

## Install

```bash
go get github.com/brualan/leaderelection
go get github.com/brualan/leaderelection/postgres   # or .../mysql
```

The core module has **no dependencies** — standard library only, tests included.
Drivers live in separate modules, so importing the PostgreSQL store never pulls
in a MySQL driver and vice versa.

## Quick start

```go
package main

import (
	"context"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := postgres.Connect(ctx, "postgres://user:pass@db:5432/app?sslmode=require")
	if err != nil {
		panic(err)
	}
	defer store.Close()

	if err := store.EnsureSchema(ctx); err != nil {
		panic(err)
	}

	elector, err := leaderelection.New(leaderelection.Config{
		Store:  store,
		Name:   "invoice-reconciler",
		Logger: slog.Default(),
		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(ctx context.Context, fence int64) {
				// Runs for the whole term, in its own goroutine. The context is
				// cancelled the instant leadership becomes uncertain -- which
				// includes the database merely becoming unreachable. Respect it
				// promptly; nobody else can lead until this returns.
				reconcileUntil(ctx, fence)
			},
			OnStoppedLeading: func() {
				slog.Info("no longer the leader")
			},
		},
	})
	if err != nil {
		panic(err)
	}

	// Blocks until ctx is cancelled. Returns nil on a clean shutdown, so it
	// drops straight into an errgroup.
	if err := elector.Run(ctx); err != nil {
		panic(err)
	}
}
```

Polling instead of callbacks works too:

```go
go elector.Run(ctx)

if elector.IsLeader() {
    // IsLeader re-checks the deadline on every call rather than trusting a
    // cached flag, so a descheduled watchdog cannot widen the window in which
    // a stale leader believes itself current.
}
```

### On MySQL

Swap the import and the constructor; nothing else in the program changes.

```go
import "github.com/brualan/leaderelection/mysql"

store, err := mysql.Connect(ctx, "user:pass@tcp(db:3306)/app")
```

It works on MySQL 8.0 and later and on MariaDB 10.5 and later, directly or
through a pooler such as ProxySQL, and it passes the same acceptance suite as
the PostgreSQL store with no exemptions. Two differences are worth knowing
about, and both are in [MySQL](#mysql) below: an operation costs two round trips
instead of one, and the table stores identities as binary.

### Behind a connection pooler

Nothing changes:

```go
store, err := postgres.Connect(ctx, "postgres://user:pass@pgbouncer:6432/app")
store, err := mysql.Connect(ctx, "user:pass@tcp(proxysql:6033)/app")
```

`Connect` configures the pool to survive transaction pooling by default. See
[PgBouncer](#pgbouncer) and [MySQL](#mysql) for what that means and why.

## Schema

```sql
CREATE TABLE IF NOT EXISTS leader_election (
    name        text        PRIMARY KEY,
    holder      text        NOT NULL,
    fence       bigint      NOT NULL,
    acquired_at timestamptz NOT NULL,
    renewed_at  timestamptz NOT NULL,
    expires_at  timestamptz NOT NULL,
    payload     bytea
) WITH (fillfactor = 70);
```

`store.EnsureSchema(ctx)` creates it — idempotent, safe on every replica on
every start. If your deployment withholds DDL rights from the application, get
the statement for your migration tool instead:

```go
ddl, _ := postgres.Schema("", postgres.DefaultTable)
```

One row per election; unrelated elections share the table. Use
`postgres.WithTable(schema, table)` for a different name.

`fillfactor` is not decoration. The row is rewritten on every renewal — several
times a minute, forever — and leaving room on the page keeps those updates HOT,
so the primary key index is untouched and autovacuum can reclaim old versions
without an index scan.

On MySQL the table is the same shape with different types:

```sql
CREATE TABLE IF NOT EXISTS leader_election (
    name        VARBINARY(255) NOT NULL,
    holder      VARBINARY(255) NOT NULL,
    fence       BIGINT         NOT NULL,
    acquired_at DATETIME(6)    NOT NULL,
    renewed_at  DATETIME(6)    NOT NULL,
    expires_at  DATETIME(6)    NOT NULL,
    payload     BLOB           NULL,
    PRIMARY KEY (name)
) ENGINE=InnoDB;
```

`mysql.Schema` and `mysql.WithTable` are the counterparts of the calls above.

## Configuration

| Field | Default | What it controls |
|---|---|---|
| `LeaseDuration` | `15s` | How long a lease stays valid. Sets worst-case failover after a crash. |
| `RenewInterval` | `5s` | How often the leader extends its lease. |
| `RetryPeriod` | `3s` | How often a follower polls. Jittered. |
| `OperationTimeout` | `3s` | Bounds a single database call. |
| `SafetyMargin` | `LeaseDuration/10`, min `1s` | How early the leader steps down. |
| `Identity` | generated | Distinguishes this participant. |
| `Payload` | none | Data published with the lease, readable by followers. |
| `DisableRelease` | `false` | Skips the release on shutdown. |

One invariant is enforced at construction, and `New` refuses a config that
breaks it:

```
RenewInterval + 2*OperationTimeout + retryDelay <= LeaseDuration - SafetyMargin
```

A leader must be able to lose one renewal outright — request sent, timeout
burned, nothing back — and still get a second attempt to *complete* before it
has to step down. Without the slack, a single slow round trip costs leadership,
and the election flaps under exactly the conditions it exists to survive.

`OperationTimeout` is charged twice on purpose: once for the attempt that is
lost and once for the attempt that has to succeed. `retryDelay` is
`max(RenewInterval/4, 50ms)`.

For participants on the far side of a WAN, raise `OperationTimeout` and
`LeaseDuration` together rather than shortening the renew interval.

### Identity

Generated as `hostname-pid-<random>`. **The random part matters.** If a
restarted process reused its predecessor's identity, it could renew the lease
that predecessor still believes it holds — and the predecessor may well be
alive, blocked in a syscall or a stop-the-world pause. Setting `Identity` to
something stable across restarts, such as a StatefulSet pod name, defeats the
guarantee.

## Fencing tokens

Stepping down promptly bounds how long a stalled leader can act. It cannot
unsend a write already in flight. If a leader is descheduled mid-write, steps
down while frozen, and the write lands after its successor started — the
election was correct and your data is still wrong.

The fix is standard and the library hands you the tool. Every term carries a
strictly increasing `fence`:

```go
OnStartedLeading: func(ctx context.Context, fence int64) {
    for job := range jobs {
        // Stamp the write, and have the receiving system reject anything
        // carrying a token older than the highest it has seen.
        if err := sink.Write(ctx, job, fence); err != nil {
            return
        }
    }
}
```

```sql
-- The receiving side, in whatever system it lives in.
UPDATE state SET value = $1, fence = $2 WHERE id = $3 AND fence <= $2;
```

No two terms ever share a token, anywhere in the election.

At the database level the token advances on one condition: reclaiming an
expired lease. That covers a new holder taking over, and equally the *previous*
holder returning to a lease it let lapse — in between, anyone could have led.
Extending a lease that never lapsed is the same term and keeps its token.

There is one case the database cannot see. A leader whose renewals stop landing
steps down locally, while its lease is still valid on the server — the local
deadline is deliberately the earlier of the two. If the network heals, that
participant could re-acquire its own untouched lease and the database would
have no reason to renumber it. The library gives the lease up instead and takes
a fresh token, so a write left in flight by the abandoned term can still be
told apart from the new one's.

## Cross-cluster leader discovery

Followers can read what the leader published, which is often the only channel
between clusters that exists:

```go
Config{Payload: []byte("https://leader.eu-west.internal:8443")}

// From any participant, leader or not:
status := elector.Status()
proxyTo(status.Leader, status.LeaderPayload)
```

## Failure modes

| What happens | What the library does | How long |
|---|---|---|
| Leader loses its network path | Steps down, cancels your context | < `LeaseDuration - SafetyMargin` |
| Database hangs without closing connections | Steps down; the watchdog does not wait on the network | < `LeaseDuration - SafetyMargin` |
| Database goes down entirely | Nobody leads | within one lease |
| PgBouncer restarts | Pools rebuild; leadership usually survives untouched | — |
| PgBouncer `PAUSE` | Nobody leads until `RESUME` | within one lease |
| Leader is killed with SIGKILL | Successor waits out the lease | ≈ `LeaseDuration` |
| Leader shuts down cleanly | Releases; successor takes over at once | ≈ `RetryPeriod` |
| Lease taken by someone else | Steps down on the failed renewal, not at the deadline | ≈ `RenewInterval` |
| Table missing, no privileges | Nobody leads; `Fatal(err)` reports it | until fixed |

Errors never come back from `Run`. They are reported through
`Status().LastError` and handled by giving up leadership rather than by giving
up. `postgres.Retryable`/`postgres.Fatal` and `mysql.Retryable`/`mysql.Fatal`
classify them for alerting: both stop the election, but only one will fix
itself.

## PgBouncer

Transaction pooling hands back a different server connection after every
transaction. Anything session-scoped breaks, so this library uses none of it:

- **No `pg_advisory_lock`.** Session-scoped, so it would break outright. Lease
  state lives in a row instead.
- **No `LISTEN`/`NOTIFY`**, no temporary tables, no `SET`.
- **No server-side prepared statements.** `Connect` defaults to
  `pgx.QueryExecModeExec` and disables the statement and description caches.
  Named prepared statements are per-session, so under transaction pooling they
  land on whichever backend the pooler hands out next and fail at random.
- **One statement per operation.** Nothing spans two transactions, so nothing
  spans two backends.

Bounded connection lifetimes and health checks are set too, so the pool sheds
connections a pooler restart left unusable instead of discovering them one
failed election at a time.

What you set in the DSN wins, with one exception: pgx cannot tell a DSN that
asked for `cache_statement` from one that said nothing, so that mode is still
moved to exec. Any other mode is left alone, and a deployment with no pooler
can build its own pool and pass it to `postgres.New`.

The acceptance suite runs **unchanged** against both paths, with
`max_prepared_statements=0` on the pooler. No exemptions: "works directly but
not through the pooler" is this library's likeliest bug, and the tests are
arranged so it cannot hide.

## MySQL

The MySQL store makes the same promises and passes the same acceptance suite.
Three things about it are worth knowing before you deploy it.

**An operation is two statements.** MySQL has no `RETURNING`, so `Acquire` and
`Renew` write and then read the row back — two round trips instead of one, and
`OperationTimeout` has to cover both. The safety argument does not change,
because a lease is reported as held only when the read-back says it is held,
never because the write succeeded. A race between the two can only ever
understate the caller's standing, and understating it is the safe direction.

**Identities are compared as bytes.** `name` and `holder` are `VARBINARY`, not
text. MySQL's default collations fold case, ignore accents and pad trailing
spaces, so on a text column `worker-a` and `worker-A` would be the same
participant — and the guard on every renewal would be looser than identity. The
suite has a scenario for each of those three, and they are the reason the column
type looks the way it does.

**Timestamps are `DATETIME(6)` holding UTC.** `TIMESTAMP` is converted using the
session's time zone, so a fleet whose connections disagreed about it would be
reading each other's timestamps shifted. Every statement takes its time from
`UTC_TIMESTAMP(6)`, which does not depend on the session at all. The store reads
the values whether or not your DSN set `parseTime`, so it works over a `*sql.DB`
the application already owns.

Everything the [PgBouncer](#pgbouncer) section says about session state applies
here too, in the MySQL dialect: no `GET_LOCK`, no user variables, no `SET`, no
temporary tables, no transactions, and no server-side prepared statements —
`Connect` interpolates parameters instead, which the integration suite verifies
by counting `Com_stmt_prepare` on the server after an election has run.

One implementation detail deserves a warning because it is invisible: the upsert
behind `Acquire` depends on MySQL evaluating assignments left to right, with a
later one seeing what an earlier one wrote. That is documented behaviour and the
integration suite asserts it against the live server, so a version that changed
it would fail loudly rather than quietly stop handing over leadership.

## Testing

Every production package is at **100% statement coverage**, enforced in CI. The
only exclusions are test-support packages, whose uncovered statements are the
assertion failures inside them — reachable only by making a test fail. The
exclusion is per package, never per line.

```bash
make test        # unit, simulation and fuzz corpora, race detector, no Docker
make test-int    # against real PostgreSQL and MySQL, direct and pooled
make test-chaos  # partitions, hangs, restarts, locks, pooler failures
make test-soak   # long randomised runs, rolling restarts, pool exhaustion
make fuzz        # explore each fuzz target (FUZZTIME=5m for a longer look)
make cover       # merged profiles, 100% floor
```

Five layers:

**Unit tests** drive the election loop on a fake clock, so timing assertions do
not depend on timing. The database's clock is deliberately offset from the
local one by three days: any code that compared them would produce nonsense and
the tests would notice.

**A simulation** runs a whole fleet on virtual time — several participants, one
shared record, and a fault schedule taken from fuzz input. It buys two things
no other layer can. Minutes of election happen in milliseconds, so a fuzzer can
explore thousands of schedules; and each participant's clock can be made to run
at a *different rate*, which is precisely what `SafetyMargin` exists to absorb.
The oracle is sampled at every step of virtual time: no two participants
leading at once, no fence token used twice, and — once the faults stop —
somebody leading again, because an election that stays safe by never electing
anyone has not survived anything.

**Fuzz targets** cover the arithmetic the safety argument rests on: the timing
invariant that decides which configurations are accepted, the lease duration
maths, the backoff delays, the quoting of a table name, the decoding of a
MySQL timestamp, and an operation-level model of the store contract that runs
against the in-memory reference and against both real databases. Their seed corpora run as ordinary tests, so every `go test`
replays them.

**The acceptance suite** (`storetest`) pins down store semantics — atomic
takeover, fence tokens that never repeat, guarded renewals that fail rather than
silently succeeding. Every implementation runs it, including the in-memory
reference, which is what keeps the guarantee from becoming dialect-specific.

**Chaos scenarios** run a real fleet against real infrastructure and break it
underneath them. Most participants share one process so their leadership intervals
can be compared directly; the safety oracle asserts that no two ever overlap and
that fence tokens never go backwards. `TestRealProcessFailure` additionally runs
independent elector processes, kills them with `SIGKILL`, or freezes them with
`SIGSTOP` until a successor takes over. After `SIGCONT`, the old process must
report no leadership or usable fence. Both databases and both poolers are tested.

| Scenario | What breaks |
|---|---|
| `TestRealProcessFailure` | actual participant process death and freeze/resume across a handover |
| `TestPoolerServerConnectionsAreExhausted` | all PgBouncer/ProxySQL server connections pinned by transactions, then released |
| `TestCascadingNetworkFailures` | degraded leader, isolated follower, total loss of replies, staggered recovery |
| `TestSteadyState` | nothing — the baseline |
| `TestLeaderIsPartitioned` | the leader's path is severed, others keep theirs |
| `TestLeaderIsBlackholed` | packets stop moving; connections stay up |
| `TestDatabaseIsPaused` | `SIGSTOP` on PostgreSQL, connections intact |
| `TestDatabaseRestarts` | PostgreSQL stopped and started |
| `TestPgBouncerRestarts` | the pooler bounced under live clients |
| `TestPgBouncerPauses` | `PAUSE` on the admin console |
| `TestLeaderCrashes` | the leader dies without releasing |
| `TestGracefulHandoverIsFast` | a clean shutdown, measuring failover |
| `TestHighLatencyIsSurvivable` | WAN latency and jitter on every path |
| `TestRepliesAreLostButRequestsArrive` | renewals land; the answers never come back |
| `TestConnectionsAreResetMidQuery` | every connection RSTs on first use |
| `TestBandwidthCollapse` | the path is up and too slow to renew over |
| `TestFlappingPath` | the leader's path cut and healed, repeatedly |
| `TestEveryoneIsPartitioned` | nobody can reach the database at all |
| `TestParticipantsSeeDifferentDatabaseClocks` | each participant is days out, differently |
| `TestTableIsLockedByAnotherSession` | `ACCESS EXCLUSIVE`, as a migration takes it |
| `TestElectionRowIsLocked` | reads work, the row cannot be written |
| `TestBackendsAreTerminated` | `pg_terminate_backend`, as a failover does |
| `TestDatabaseGoesReadOnly` | writes fail with 25006, reads do not |
| `TestPostgresIsKilled` | `SIGKILL` and crash recovery |
| `TestRandomisedSoak` | a minute of randomly injected faults |

The MySQL store gets the same treatment against MySQL and ProxySQL, plus two
scenarios that only mean something for a store whose operations are two
statements:

| Scenario | What breaks |
|---|---|
| `TestMySQLSteadyState` | nothing — the baseline |
| `TestMySQLLeaderIsPartitioned` | the leader's path is severed, others keep theirs |
| `TestMySQLLeaderIsBlackholed` | packets stop moving; connections stay up |
| `TestMySQLLeaderSeesNoReplies` | renewals commit; the answers never come back |
| `TestMySQLConnectionsDieMidOperation` | connections RST between the write and the read-back |
| `TestMySQLTableIsLocked` | `LOCK TABLES ... WRITE`, as a migration takes it |
| `TestMySQLElectionRowIsLocked` | reads work, the row cannot be written |
| `TestMySQLSessionsAreKilled` | `KILL`, as a failover does |
| `TestMySQLGoesReadOnly` | writes fail with 1290, reads do not |
| `TestMySQLIsPaused` | `SIGSTOP` on MySQL, connections intact |
| `TestMySQLIsKilled` | `SIGKILL` and InnoDB crash recovery |
| `TestProxySQLRestarts` | the pooler bounced under live clients |
| `TestMySQLLeaderCrashes` | the leader dies without releasing |
| `TestMySQLGracefulHandoverIsFast` | a clean shutdown, measuring failover |
| `TestMySQLParticipantsSeeDifferentDatabaseClocks` | each participant is days out, differently |

The lease-boundary regressions, process-failure scenarios, and their assumptions
are described in [the resilience testing audit](RESILIENCE_TESTING.md).

Most run against both the direct and the pooled path. Longer scenarios live
behind `make test-soak`: a randomised soak that also replaces participants
mid-flight, a rolling restart of the whole fleet, more participants than the
pooler has server connections, and two elections sharing one table while one of
them is broken.

## Testing your own application

An election backed by `memstore` exercises the real elector — promotion,
renewal, watchdog, fencing — without a database:

```go
import "github.com/brualan/leaderelection/memstore"

elector, _ := leaderelection.New(leaderelection.Config{
    Store: memstore.New(),
    Name:  "test",
})
```

It elects among goroutines in one process, so it is not a substitute for the
real stores.

## Operating it

```sql
-- Who leads what, and for how long.
SELECT name, holder, fence,
       expires_at - now() AS remaining,
       now() - acquired_at AS term_age
FROM leader_election;
```

Worth alerting on: `remaining` persistently negative (nobody is leading),
`fence` climbing quickly (leadership is flapping — usually the timing invariant
is too tight for your latency), and any error where `postgres.Fatal` is true.

The table is small and hot. `fillfactor = 70` is set by `EnsureSchema`; if you
run the DDL yourself, keep it.

## Compared with

| | Trade |
|---|---|
| **Kubernetes Lease** | Better inside one cluster. Cannot span clusters at all. |
| **etcd / ZooKeeper / Consul** | Real consensus, no database SPOF — and another cluster to operate. |
| **Redis Redlock** | Contested safety argument on a store without durable, linearizable writes. |
| **`pg_advisory_lock`** | Simpler, and unusable behind a transaction pooler; a dropped session releases the lock silently, with no fencing token. |

Pick this one when the database is already there and already the thing you
cannot lose.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The short version: `make check` must
pass, coverage stays at 100%, and a change to the election loop needs a chaos
scenario that would have caught the bug it fixes.

## License

[MIT](LICENSE).
