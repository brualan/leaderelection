# Contributing

## Getting set up

```bash
git clone https://github.com/brualan/leaderelection
cd leaderelection
make up      # PostgreSQL, MySQL, their poolers and Toxiproxy in Docker
make check   # vet, gofmt, golangci-lint, unit tests
```

Go 1.23 or newer, plus Docker for anything past `make check`.

The repository is a Go workspace of five modules:

| Module | Contents | Dependencies |
|---|---|---|
| `.` | the election itself | none, tests included |
| `./postgres` | the PostgreSQL store | pgx |
| `./mysql` | the MySQL store | go-sql-driver/mysql |
| `./tests` | chaos and end-to-end scenarios | the three above |
| `./tools` | repository tooling | none |

The core module's empty dependency list is a feature, not an accident. A change
that adds a dependency to it — including a test dependency — needs a very good
reason. `internal/assert` exists so that the tests need no assertion library.

`depguard` enforces this import policy for the core module, including tests;
the database, scenario and tooling modules are exempt from that rule.
`errcheck` also checks errors assigned to `_`. Handle errors explicitly, or
use a local `//nolint:errcheck // reason` when ignoring one is intentional.
`nolintlint` requires every suppression to name its linter and explain why,
and rejects unused suppressions. Test helpers must call `t.Helper()` (or
`tb.Helper()` for `testing.TB`) so failures point to their callers.

## Running the tests

```bash
make test        # unit, simulation and fuzz corpora, race detector, no Docker
make test-int    # against real PostgreSQL and MySQL, direct and pooled
make test-chaos  # partitions, hangs, restarts, locks, pooler failures
make test-soak   # long randomised runs, rolling restarts, pool exhaustion
make test-extreme # at least 30m of compound faults; see EXTREME_CHAOS.md
make test-all    # everything except the soak
make cover       # merged profiles with a 100% floor
```

Do not run chaos or soak suites concurrently against the same Toxiproxy.
Each suite clears existing proxies at startup, and database faults affect the
whole shared infrastructure. To compile tagged tests without touching running
scenarios, use `go test -c -tags=chaos -o /tmp/leaderelection-chaos.test ./tests`.

`make test-chaos` takes a few minutes; the soak scenario alone runs for a
minute. `go test -short` skips it.

## Fuzzing and soaking

```bash
make fuzz                 # every target for 30s each
make fuzz-int             # store model through real PgBouncer and ProxySQL
FUZZTIME=5m make fuzz     # a longer look
LEADERELECTION_SOAK_DURATION=30m make test-soak
```

Nothing here runs in CI: both are for going looking, not for gating a change.
What CI does get for free is the **seed corpora** — `go test` replays every
fuzz seed and every input committed under `testdata/fuzz/`, so a bug found once
stays found. When a target fails, Go writes the input there; commit it with the
fix.

The targets worth knowing about:

| Target | Subject |
|---|---|
| `FuzzElectionUnderFaults` | a whole fleet on virtual time, against a schedule of faults |
| `FuzzConfigValidate` | every configuration `New` accepts survives a lost renewal |
| `FuzzLeaseArithmetic` | `TTL`, `Expired` and `HeldBy` stay mutually consistent |
| `FuzzBackoffNext`, `FuzzJitter` | delays are never negative and never exceed their ceiling |
| `FuzzQualify` | a table name that is accepted is still one identifier |
| `FuzzClassification` | `Retryable` and `Fatal` stay disjoint under wrapping |
| `FuzzStore` | arbitrary operation sequences against a model of the contract |

`FuzzStore` runs against `memstore` at tens of thousands of sequences a second
and against real PostgreSQL, through PgBouncer, under `-tags=integration`. The
model lives in `storetest/fuzz.go`, so a second dialect inherits it.

`FuzzElectionUnderFaults` is deterministic in virtual time but not in goroutine
scheduling. A reproducer usually fails on the first run and occasionally does
not; re-run it with `-count 50` before concluding it was a fluke. The failure
message carries a journal of the fault schedule, which is the only readable
form a schedule that long has.

## What a change needs

**Coverage stays at 100%** for every production package. `make cover` enforces
it. If a line is genuinely unreachable, that is a design problem to fix rather
than a line to exclude — see `identity.go` and `store.go` in the postgres module
for how an infallible system call is made testable by indirecting it through a
package variable. Per-line exclusions are not available, on purpose.

**A change to the election loop needs a test that would have caught the bug.**
For safety-relevant changes that means a chaos scenario, not a unit test:
`tests/chaos_test.go` breaks the path to the database and `tests/chaos_db_test.go`
breaks the database itself, both underneath a real fleet, and the safety oracle
in `tests/harness` is what actually checks the guarantee. A change to the
*timing* of the loop should also come with a simulation scenario in
`simulation_scenarios_test.go`, where clock rates can be made to disagree.

**A scenario must be able to fail.** Every one of them ends by asserting that
the fault it injected actually bit — a term count, a step-down, a number of
sessions killed. A scenario that passes with the fault commented out is worse
than no scenario, because it looks like coverage.

**A change to store semantics belongs in `storetest`.** That suite is the
contract every implementation is held to; adding a case there is how a
behaviour becomes required rather than incidental.

**Comments explain why, not what.** The SQL in `postgres/sql.go` and the
watchdog in `elector.go` are the two places where the reasoning is not
recoverable from the code, and they are commented accordingly. Match that.

## Releasing

The submodules require the core module by version, as published modules must,
which means tags have to go out in order:

```bash
git tag v0.2.0            && git push origin v0.2.0             # core first
# bump the require in postgres/go.mod to v0.2.0, commit
git tag postgres/v0.2.0   && git push origin postgres/v0.2.0
```

Until the core module has the tag a submodule requires, `go mod tidy` cannot
resolve it — `go.work` bridges the gap for local work, and the submodules'
`go.mod` files are maintained by hand. `make tidy` skips them for that reason.

`./tests` and `./tools` are never published.

## Adding a store

1. Implement `leaderelection.Store` in a new module.
2. Run `storetest.Run` against it, with no exemptions.
3. Run it through a connection pooler as well as directly, as a subtest of the
   same suite — see `postgres/integration_test.go`.
4. Add it to the chaos matrix in `tests/`.

The semantics that are easy to get wrong, and that `storetest` will catch:
takeover has to be atomic, the fence has to advance when an expired lease is
reclaimed *even by its previous holder*, and every timestamp has to come from
one clock read on the server.
