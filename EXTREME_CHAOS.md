# Extreme chaos campaign

`make test-extreme` runs clock-boundary checks and then **at least 30 minutes of
real infrastructure fault scenarios**, with the Go race detector. Setup and
compilation do not count towards that minimum. The campaign fails immediately
on a safety/liveness violation; a failed short attempt is not reported as a
successful 30-minute run.

```bash
mkdir -p test-results
make test-extreme > test-results/extreme.log 2>&1
# Optional reproducible scenario order and a longer duration:
LEADERELECTION_EXTREME_SEED=2026102001 \
LEADERELECTION_EXTREME_DURATION=45m make test-extreme
```

Create `test-results` before redirecting output. It is ignored by Git and is
separate from `.cover`, which `make cover` deletes. Do not run other chaos/soak
binaries concurrently: their TestMain deletes proxies used by this campaign.
The combined build tags are intentional; a regular chaos or soak run does not
include this expensive campaign.

Each complete cycle contains all 23 scenario families in seeded random order.
The first cycle always runs in full; later cycles stop after the current family
finishes once the minimum duration is reached. Every family uses its own test
cleanup, and any failed family aborts the campaign. In addition to the existing
network/database fault matrix the campaign adds:

- 48 candidates held at an Acquire barrier, released together, followed by six
  clean leader shutdowns and contested reacquisitions on each of four routes.
  Logs include peak concurrent acquires and handover latency.
- 16 participants per route, with simultaneous asymmetric reply loss, jittered
  latency above operation timeout, TCP resets, bandwidth collapse, and a fully
  blackholed incumbent. The incumbent is replaced before all paths are healed.
  Participants also receive database timestamps offset by days in both
  directions; this tests offset independence, not arbitrary clock-rate drift.
- Eight participants behind each pooler while the pooler is frozen, the leader
  loses replies, and database sessions are killed. The database and pooler are
  then both killed without graceful cleanup and restarted under live clients.
- An old writer that intentionally ignores cancellation. An in-memory receiver
  atomically activates a newer fence and rejects stale writes, while allowing
  current writes. This models receiver enforcement; it is not an integration
  test of Kafka/S3/HTTP enforcing tokens automatically.
- Concurrent metrics collection while the leader loses its database replies,
  checking snapshot invariants, timeout counters, self-fencing, and recovery.
- Complete backend connection starvation in both poolers, real elector process
  SIGKILL and SIGSTOP/SIGCONT, table/row locks, read-only databases, partitions,
  flapping, rolling replacements, and independent elections sharing a table.

Safety is checked using callback intervals, strictly increasing fencing tokens,
and sampled public leadership status. Recovery has explicit deadlines so that
"nobody leads forever" cannot pass. Race detector failures also fail the run.

## Clock assumptions are tested explicitly

The core clock tests run 20 times before the real-infrastructure campaign.
Supported bounded drift and lease deadline regressions must remain safe.
Two **characterization** tests deliberately violate the clock assumptions:

- The simulated database jumps forward while the incumbent's monotonic clock
  does not. The database can hand the lease to a successor while the old node
  still considers itself leader.
- The incumbent's monotonic clock runs at half speed, well beyond what the
  safety margin covers, while its database path is lost. The old local lease
  and the successor's term overlap.

These tests pass only when they reproduce that documented unsafe boundary.
They do not mean the library withstands these faults. The process/receiver tests
separately check stale fence rejection. No host or container wall clock is
changed. Database split brain, lost committed fencing history, and physical
host power cuts are not emulated by this campaign.

The seed reproduces scenario order; Go scheduling, jitter, SQL scheduling, and
network timing remain nondeterministic. Save the full log and the source
revision along with a failure report.

## Result: 2026-10-20

The campaign passed with exit code 0. Fault scenarios ran for **30 minutes
23.708 seconds** (1,823.708 seconds); compilation and setup are excluded.
Seed: `2026102001`; base library commit: `5ecfab6`. Source hashes were unchanged
throughout the run.

- 80 scenario-family executions across all 22 families: three complete cycles
  plus part of a fourth, in different seeded orders.
- 241 safety-oracle checks; zero failed tests and zero race detector warnings.
- All observed synchronized herd peaks reached 48 concurrent acquires.
- 72 clean-release handovers; maximum measured handover latency 469.040 ms.
  This measures this fixture's clean handover, not a production failover SLA.
- The receiver model rejected 60,903 stale writes. Real external services must
  implement the corresponding atomic fencing check themselves.
- Clock characterization and bounded-drift/deadline checks passed 20 repetitions
  with the race detector before the campaign. The two unsupported clock faults
  intentionally reproduced unsafe leadership overlap, as described above.
- Final `make check` passed (vet, formatting, race-enabled unit suites).
  Optional `golangci-lint` was unavailable. Test containers were stopped with
  data preserved after the campaign.
- A preliminary run of the four new infrastructure families also passed
  separately in 170.529 seconds.

Local evidence (ignored by Git):

- `test-results/extreme-20261020.log`: complete campaign output.
- `test-results/extreme-20261020.summary.json`: machine-readable results.
- `test-results/extreme-20261020.meta.json`: command, seed, source hashes.
- `test-results/extreme-20261020.docker.log`: final container log excerpts.
- `test-results/extreme-smoke.log`: preliminary run.

No production election code was changed for this campaign. The result supports
the listed behavior under the documented clock/database assumptions. It does
not make unbounded clock drift, forward database-clock jumps, lost committed
fencing history, or an unfenced external receiver safe.

## Result: 60-minute campaign, 2026-10-20

The campaign passed with exit code 0 after **60 minutes 7.087 seconds**
(3,607.087 seconds) of fault scenarios, excluding setup and compilation.
Seed: `2026102060`; base commit: `a5dbdd6`. All recorded source hashes
remained unchanged while the campaign ran.

- 159 scenario-family executions across all 23 families: six full cycles and
  21 families from the seventh cycle.
- 482 safety-oracle checks; zero failed tests and race detector warnings.
- All 24 synchronized herd measurements reached 48 concurrent acquires.
- 144 clean-release handovers; maximum measured latency 457.170 ms.
- The fenced receiver model rejected 106,611 stale writes from a writer that
  deliberately ignored cancellation. These are rejected application attempts,
  not simultaneous valid leases issued by the library.
- Clock checks passed 20 race-enabled repetitions, with the same unsupported
  clock boundaries described above.
- The additional partial-stream test passed **all eight cases** separately,
  in 42.87 seconds (43.898 seconds including race-detector shutdown). Every
  case forwarded exactly two bytes, recorded renewal timeouts, stopped the old
  callback, acquired a newer fence, and recovered after healing. No overlap
  was observed. This supplement was not included in the 60-minute binary.

Final `make check` and tagged `go vet` passed. Optional `golangci-lint` was
unavailable. Test containers were stopped with data preserved.

Evidence is saved locally under `test-results/extreme-60m-20261020T132847Z`:
`.log`, `.meta.json`, `.summary.json`, `.partialstall.log`, `.check.log`, and
`.docker.log`. These files are ignored by Git. Production library code was
not changed. The clock and receiver assumptions above still apply.

## Partial protocol exchange followed by a silent stall

`TestPartialProtocolBytesThenSilentStall` adds a real TCP relay in front of
each of the four database routes. It first establishes a healthy leader and
accepts a renewal. It then forwards exactly two application-stream bytes in
one direction and silently discards subsequent payload in that direction.
There is no injector timeout, EOF, or RST; the opposite direction stays open.
Request and reply stalls are separate cases, for eight cases total.

The checks require the driver operation to finish through client cancellation
or timeout, the local leader and its callback to stop, a healthy successor to
obtain a newer fence, and the old participant to recover after healing without
reviving its stale term. Socket teardown during test cleanup cannot satisfy
these assertions. This models stalled protocol traffic, not a specific
middlebox implementation.

The test was written after the 60-minute campaign had started and is a separate
supplement; it is not part of that campaign's running binary. Future ordinary
`make test-chaos` runs include it. To run just this test against the local
Compose fixtures without invoking the shared chaos proxy reset:

```bash
make up
cd tests
go test -tags=partialstall -race -count=1 -timeout=5m -v \
  -run '^TestPartialProtocolBytesThenSilentStall$' .
```

Run it sequentially with the other database fault suites.
