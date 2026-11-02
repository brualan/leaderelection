# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses
[semantic versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `IsLeader`, `Fence`, and `Status` read the clock under the mutex now. A
  caller stalled on the lock was previously judged against a stale timestamp
  and could be answered "leader" after its deadline had passed.
- `New` copies `Config.Payload`; mutating the caller's slice after
  construction no longer changes what the elector publishes.
- Documented that the next leader's `OnStartedLeading` may overlap the
  previous one: waiting for the local callback to return is not a global
  handover barrier. Fencing tokens remain the mechanism that makes overlap
  safe.
- Documented the durability precondition behind fencing tokens: a failover
  to a lagging replica or a restore from an old backup rewinds the fence
  counter and can reissue a token (`memstore/fencing_rewind_test.go` pins
  the consequence).
- `mysql` and `postgres` go.mod files now require a resolvable version of
  the core module and ship a `go.sum`; previously the modules could not be
  built outside this workspace at all.

### Added

- CI builds and tests every module with `GOWORK=off`, plus a job compiling a
  throwaway consumer against the three published modules, so go.mod drift
  can no longer hide behind the workspace.
- MariaDB 10.5, 10.11, and 11.4 joined the MySQL integration matrix; the
  support claim in the README is now backed by CI.
- Regression tests: `TestLeadershipChecksReadTimeUnderLock`,
  `TestNewClonesPayload`, `TestNextLeaderDoesNotWaitForPredecessorCallback`.

### Added

- Offline `SchemaSQL(opts...)` and read-only `Store.CheckSchema(ctx)` for both SQL
  stores. Deployment examples now use external migrations and DML-only service
  credentials; `EnsureSchema` is deprecated and retained for compatibility.

- Leader election over a shared PostgreSQL or MySQL instance, for replicas
  spread across clusters that cannot reach each other.
- Safety through database-side timestamps, conservative local deadlines, and a
  watchdog that steps down without waiting on the network.
- Fencing tokens for guarding writes to systems outside the election.
- `postgres` store, verified against PostgreSQL directly and through PgBouncer
  in transaction pooling mode.
- `mysql` store, verified against MySQL 8.0 and 8.4 directly and through
  ProxySQL. MySQL has no `RETURNING`, so `Acquire` and `Renew` write and then
  read the row back; a lease is reported as held only when that read says so,
  which keeps a race between the two statements on the safe side. Identities are
  stored as `VARBINARY` because MySQL's default collations would otherwise make
  two distinct participants compare equal, and timestamps are `DATETIME(6)`
  holding `UTC_TIMESTAMP(6)` so that nothing depends on a session time zone.
- `memstore`, an in-process store for testing applications that use the
  library.
- `storetest`, the acceptance suite every store implementation must pass.
- Chaos suite covering partitions, hung databases, restarts and pooler
  failures, with a safety oracle that checks leadership intervals for overlap.
- Chaos scenarios for faults inside the database rather than on the way to it:
  a table lock, a row lock, terminated backends, a read-only server, and a
  PostgreSQL killed rather than stopped.
- The same chaos scenarios against MySQL and ProxySQL, plus two that only mean
  something for a store whose operations are two statements: a leader whose
  renewals commit but whose replies never arrive, and connections that reset
  between the write and the read-back.
- Chaos scenarios for one-way blackholes, connection resets mid-query,
  bandwidth collapse, a flapping path, a total partition, and participants who
  each read the database's clock as something different.
- Soak suite behind `make test-soak`: randomised faults with participants being
  replaced mid-flight, a rolling restart of the whole fleet, more participants
  than the pooler has server connections, and two elections sharing a table.
- A simulation of the whole election on virtual time, with per-participant
  clock *rates*, driving `FuzzElectionUnderFaults`. Safety is sampled at every
  step of virtual time; liveness is asserted once the faults stop.
- Fuzz targets for the timing invariant, lease arithmetic, backoff delays,
  table-name quoting, error classification, MySQL timestamp decoding, and an
  operation-level model of the store contract that runs against `memstore` and
  against both real databases.

### Fixed

Found by review before the first release, so there is nothing to upgrade from.
They are recorded because each one is a trap the next implementation of this
idea will walk into.

- A fence token could start a second term. After a local step-down the lease is
  still valid in the database -- the local deadline is deliberately earlier --
  so re-acquiring it gave the store no reason to renumber. The lease is now
  given up first, and the next acquisition mints a fresh token.
- The timing invariant charged `OperationTimeout` once when the worst case
  spends it twice: on the renewal that is lost and on the one that has to
  succeed. Configurations with a long operation timeout passed validation and
  then flapped.
- `postgres.Retryable` matched a closed pool by error message, which stopped
  working the moment the error was wrapped -- which this package always does.
  It now matches `puddle.ErrClosedPool` through `errors.Is`.
- `Status` handed out the elector's own payload slice, so one caller's edit
  surfaced in another's snapshot.
- A store reporting more time remaining than was asked for could push the local
  deadline past the lease's real expiry. The granted TTL is now clamped.
- `Config.Validate` accepted configurations whose renewal budget overflowed a
  duration: the sum wrapped negative and compared as though there were time to
  spare, waving through exactly what the check exists to reject. Found by
  `FuzzConfigValidate`.
- A backoff delay computed near the top of the duration range converted through
  a `float64` that rounds *above* the largest `time.Duration`, wrapping to the
  most negative one. A timer armed with it fires immediately, turning the retry
  it was pacing into a spin against a database already in trouble. Reachable
  with a large `LeaseDuration`, since that is the backoff ceiling. Found by
  `FuzzBackoffNext`.
