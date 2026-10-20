# Resilience testing audit

The additional tests target lease expiry, uncertain network outcomes, complete
participant process failure, and recovery after overlapping faults. They run
with the existing PostgreSQL/MySQL and direct/pooled test matrix.

## Bugs reproduced and fixed

- Acquisition could start application work after network delay or an
  `OnNewLeader` callback had consumed the conservative local lease. Promotion
  now checks the local deadline immediately before installing the term.
- Acquisition trusted an inflated server TTL, although renewal already capped
  it. Both paths now cap the TTL at the requested lease duration.
- A late successful renewal could make an expired local term valid again if
  the watchdog had not been scheduled. Renewal now checks the previous local
  deadline before extending it and ends expired terms.
- Cancellation during an observation callback could still start a leadership
  term. Promotion now checks the Run context too.

`resilience_test.go` reproduces these failures without scheduler luck, including
exact expiry boundaries and a deliberately absent watchdog. It also checks that
an ordinary shutdown racing with successful renewal still releases the lease.
`TestSimulatedLatencyAroundOperationTimeout` exercises a four-node election
with network delay just below, at, and above the operation timeout.

The pre-existing untracked cron/Kafka examples also needed qualified package
names, valid example names, deterministic leader/follower startup, and explicit
shutdown synchronization. Otherwise a follower could win first and leave an
executable example waiting forever for the intended leader. Cancellation tests
also exposed a cron shutdown waiting on a context cancelled only by its own
unreachable defer. The scheduler now cancels daily work with leadership and
joins actual task goroutines. The Kafka example joins its message producer and
makes blocked sends cancellable. These are local examples, not real Kafka
integration tests.

## Additional real-infrastructure scenarios

| Scenario | Injection | Required result |
|---|---|---|
| `TestRealProcessFailure/*/kill` | SIGKILL of an independent elector process | A successor leads with a greater fence, without a release from the dead process |
| `TestRealProcessFailure/*/freeze` | SIGSTOP until takeover, then SIGCONT | The resumed process exposes neither leadership nor a valid fence through any public status API; the successor remains stable |
| `TestPoolerServerConnectionsAreExhausted` | Pin all 5 PgBouncer / 50 ProxySQL backend connections in transactions | No leader while starved; recovery with a fresh fence after releasing transactions |
| `TestCascadingNetworkFailures` | Leader latency exceeding timeout plus follower partition; successor reply loss; staggered healing | The sole reachable candidate takes over, nobody leads during the total outage, recovery gets a fresh fence and stabilizes |

The process and cascading network scenarios run against PostgreSQL directly,
PgBouncer, MySQL directly, and ProxySQL; connection starvation targets the two
poolers. The existing suites additionally cover database and pooler
restarts, database pauses, read-only operation, session termination, table/row
locks, blackholes, TCP resets, bandwidth collapse, jitter, flapping, and lost
responses after committed writes.

## Completed checks

- `make check`: passed, including the race detector in all three library modules.
- All 11 fuzz targets: passed with 10 seconds of exploration each.
- Additional election fault fuzzing: passed, 10,687 executions in 121 seconds.
- Real pooled store-model fuzzing (`make fuzz-int FUZZTIME=30s`): passed,
  1,300 sequences through PgBouncer and 1,689 through ProxySQL.
- Lease/cancellation regressions: repeated 30 times with the race detector;
  deadline-specific checks also passed an earlier 50-repeat run.
- Timeout-boundary simulation: passed 20 repetitions with the race detector.
- Executable examples and example cancellation checks: passed 20 repetitions.
- PostgreSQL and MySQL integration suites: passed, direct and pooled.
- `make cover`: passed, 100.0% across all six production packages.
- Actual backend connection starvation: passed for PgBouncer (8.81 seconds)
  and ProxySQL (8.69 seconds), with recovery and fence checks.
- Full soak suite: passed in 405.084 seconds, including a five-minute random
  fault schedule with 18 participants replaced and 24 leadership terms, rolling
  restarts, pool contention, and independent elections. Real process failures
  also ran again against the final election implementation.
  Churn seed: `1792495881752747619`.
- Full chaos suite: passed, 40 top-level tests and 34 subtests in 555.779 seconds;
  includes all eight real process failure cases and all four cascading network
  failure routes.

## Local environment

The audit used Go 1.26.8 on Linux/amd64, `postgres:17-alpine`, `mysql:8.0`,
`edoburu/pgbouncer:latest`, `proxysql/proxysql:2.6.6`, and
`ghcr.io/shopify/toxiproxy:2.12.0`. Mutable image tags describe this local run,
not a pinned database version matrix. `go vet` ran for every module; the optional
`golangci-lint` executable was not installed.

## Reproducing the checks

```bash
make check
make fuzz FUZZTIME=10s
go test -run '^$' -fuzz '^FuzzElectionUnderFaults$' -fuzztime=120s -parallel=4 .
make test-int
make fuzz-int FUZZTIME=30s
make test-chaos
make test-soak
make cover
```

Run infrastructure suites sequentially. Every chaos/soak binary clears
Toxiproxy on startup, and database faults affect all participants using that
infrastructure. A second suite, even one started with `-run '^$'`, would still
execute TestMain and delete the first suite's proxies. Compile without running
with `go test -c -tags=chaos -o /tmp/leaderelection-chaos.test ./tests`.

## Scope of the evidence

SIGKILL tests actual process death without cleanup; it does not power off a
physical host. The network tests inject TCP faults through Toxiproxy rather
than emulate every router or kernel failure. Tests use the local compose
versions; the existing CI database version matrix remains necessary.

The database must remain a single consistent authority and retain committed
lease/fence history. Asynchronous replication failover that loses that history,
multiple writable primaries, database clock jumps forward, and an external
system that ignores fences cannot be made safe by this library alone.
Application callbacks must honor cancellation; a frozen runtime cannot execute
cancellation until it resumes. External writes need receiver-enforced fencing.
The tests establish the listed properties under these assumptions, not immunity
to every possible failure.
