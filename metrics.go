package leaderelection

import (
	"context"
	"errors"
	"time"
)

// Metrics is an in-memory snapshot of one Elector. It requires no exporter and
// performs no Store calls. Counters belong to this object, survive repeated
// Run calls, and reset only when a new Elector is constructed. They are local
// to a replica, not global election totals. Export counters with CounterValue
// semantics (do not Add the whole snapshot on every scrape).
//
// Booleans are gauges: export false as 0 and true as 1. Durations are already
// seconds. Use a bounded election label and the monitoring system's instance
// label; use Status/logs for identities, fence tokens, payloads and error text.
// Scrapes of different replicas are not simultaneous: summing IsLeader can
// briefly appear to show two leaders during a healthy handover.
//
// A snapshot is for monitoring, not permission to perform leader work. Use the
// callback context and receiver-enforced fencing for that.
type Metrics struct {
	// Election is Config.Name. Suitable as a label if the application bounds
	// the number of election names; never generate an election per request.
	Election string
	// Running is true until Run finishes, including callback teardown/release.
	// Export as a gauge; a running follower is healthy, not an error.
	Running bool
	// IsLeader rechecks the conservative deadline at snapshot time. Export as
	// leader_election_is_leader. It can become false before the watchdog runs.
	IsLeader bool
	// LeaseRemainingSeconds is the time left before local step-down, after
	// subtracting SafetyMargin, not the database's full TTL. Zero when not leader.
	// Export as a gauge; evaluate low values only while IsLeader is true.
	LeaseRemainingSeconds float64
	// HasObservation distinguishes an unobserved election from a very recent
	// observation. Export as a gauge; omit the age sample until this is true.
	HasObservation bool
	// ObservationAgeSeconds is age of the last lease observed by Inspect,
	// Acquire or Renew, including observations of another/expired holder. Zero
	// before the first observation. It is not age of the last valid renewal.
	// Export as a gauge; choose an alert threshold above the polling/backoff.
	ObservationAgeSeconds float64
	// HasSuccessfulRenewal says whether the last installed local term accepted
	// a renewal. Reset to false on every promotion; retained after step-down.
	// Export as a gauge; omit the renewal-age sample until true.
	HasSuccessfulRenewal bool
	// LastSuccessfulRenewalAgeSeconds is age of the last renewal accepted in
	// the last installed local term. Zero until that term accepts a renewal.
	// Export as a gauge; gate alerts on IsLeader and HasSuccessfulRenewal.
	// Also monitor LeaseRemainingSeconds for a never-renewed term.
	LastSuccessfulRenewalAgeSeconds float64
	// HasError mirrors Status.LastError != nil, cleared by successful observation.
	// Export as a gauge; short failures may disappear between scrapes. Use Store
	// error counters to count operations, not this flag or error text labels.
	HasError bool
	// LeaderWorkActive means OnStartedLeading is scheduled or has not returned.
	// Export as a gauge. It may be false while IsLeader is true if the callback
	// returns early. It cannot track goroutines detached by application code.
	LeaderWorkActive bool
	// LeaderWorkCanceled means the outstanding leader callback's context has
	// been canceled. False when no callback is outstanding. Export as a gauge;
	// a sustained value indicates slow/stuck cooperative shutdown, not permission
	// to start a replacement locally. Process suspension delays all monitoring.
	LeaderWorkCanceled bool
	// TermsStartedTotal counts installed local terms, before callback scheduling.
	// Export as a counter; its rate detects local leadership churn.
	TermsStartedTotal uint64
	// TermsEndedTotal counts local step-down transitions, once per term, before
	// waiting for application work. Export as a counter. It can lag deadline
	// expiry when the watchdog is descheduled; it does not mean work has stopped.
	TermsEndedTotal uint64
	// LeaseLossesTotal counts terms ended with ErrLeaseLost (including foreign
	// holder/fence replies). Export as a counter; it is a subset of TermsEndedTotal.
	LeaseLossesTotal uint64
	// LocalExpirationsTotal counts terms ended without timely confirmation, with
	// ErrLeaseExpiredLocally. Export as a counter and investigate increases with
	// Store errors/latency. It is a subset of TermsEndedTotal, not a DB-error count.
	LocalExpirationsTotal uint64
	// RenewalsAcceptedTotal counts renewal responses accepted for a still-live
	// local term, even if an older response leaves its deadline unchanged.
	// Export as a counter. A nil Store.Renew error alone is not acceptance: the
	// reply can be foreign, stale, or arrive after local expiry.
	RenewalsAcceptedTotal uint64
	// LeaderChangesObservedTotal counts changes to observed holder or fence,
	// including the first observation and expired leases. Export as a counter;
	// do not sum replicas to count global handovers: they observe the same change.
	LeaderChangesObservedTotal uint64
	// Store contains statistics at the Store interface boundary. Export with a
	// fixed operation label (inspect/acquire/renew/release), not SQL/error text.
	// Measurements include driver/pool wait and retries inside the Store call.
	Store StoreMetrics
}

// StoreMetrics has a fixed shape to avoid unbounded labels and mutable maps.
type StoreMetrics struct {
	// Inspect measures Store.Inspect; ErrNoLease is an expected outcome.
	Inspect OperationMetrics
	// Acquire measures Store.Acquire; a foreign holder with nil error is normal
	// contention, counted as a completed call, not an error or a won term.
	Acquire OperationMetrics
	// Renew measures Store.Renew; ErrLeaseLost is an expected protocol outcome,
	// tracked separately from operational errors. See LeaseLossesTotal too.
	Renew OperationMetrics
	// Release measures Store.Release, including cleanup after cancellation and
	// release of spent fences. A nil error need not mean a row was changed.
	Release OperationMetrics
}

// OperationMetrics counts completed calls by their normalized returned error.
// For example a timeout mapped from context.Canceled is a timeout, whereas a
// parent cancellation remains a cancellation. A nil error is counted as success
// even if cancellation raced with it. These counters do not classify SQL error
// codes or decide whether a returned lease was usable.
type OperationMetrics struct {
	// CallsTotal counts completed calls of every outcome. Export as a counter;
	// stuck calls are visible in InFlight, not yet included here.
	CallsTotal uint64
	// ErrorsTotal counts operational errors, including DeadlineExceeded. It
	// excludes Canceled, ErrNoLease and ErrLeaseLost. Export as a counter; rate
	// divided by rate(CallsTotal) gives the operational error fraction.
	ErrorsTotal uint64
	// TimeoutsTotal counts DeadlineExceeded and is a subset of ErrorsTotal.
	// Export as a counter; do not add it to ErrorsTotal when computing totals.
	TimeoutsTotal uint64
	// CanceledTotal counts context.Canceled, usually shutdown/watchdog cleanup.
	// Export as a counter; do not treat all cancellations as database outages.
	CanceledTotal uint64
	// ExpectedOutcomesTotal counts ErrNoLease and ErrLeaseLost. Export as a
	// counter with operation label; absent lease and lost lease are not network
	// failures. Outcomes are disjoint from ErrorsTotal and CanceledTotal.
	ExpectedOutcomesTotal uint64
	// InFlight counts calls started but not completed. Export as a gauge. A
	// sustained nonzero value after step-down can expose an uncooperative Store.
	InFlight uint64
	// DurationSecondsTotal sums elapsed local monotonic time for all completed
	// calls, including failed/canceled calls and pool wait. Export as a counter;
	// rate(DurationSecondsTotal)/rate(CallsTotal) estimates mean call duration.
	// This aggregate cannot provide histogram buckets or p95/p99 quantiles.
	DurationSecondsTotal float64
	// LastDurationSeconds is duration of the most recently completed call, or
	// zero before any completion. Export as a gauge for diagnostics; scrapes miss
	// intermediate calls, so this is not a substitute for a latency histogram.
	LastDurationSeconds float64
}

type metricCounters struct {
	termsStarted, termsEnded, leaseLosses, localExpirations uint64
	renewalsAccepted, leaderChangesObserved                 uint64
	store                                                   StoreMetrics
	lastRenewalAt                                           time.Time
}

// Metrics returns a copied snapshot without I/O or callbacks. It is safe to
// collect concurrently with Run; callers cannot mutate the underlying counters.
// Reading metrics never resets counters, renews a lease, or triggers step-down.
func (e *Elector) Metrics() Metrics {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.clock.Now()
	c := &e.metrics
	m := Metrics{
		Election: e.cfg.Name, Running: e.running.v.Load(), HasError: e.lastErr != nil,
		HasObservation: !e.observedAt.IsZero(), HasSuccessfulRenewal: !c.lastRenewalAt.IsZero(),
		TermsStartedTotal: c.termsStarted, TermsEndedTotal: c.termsEnded,
		LeaseLossesTotal: c.leaseLosses, LocalExpirationsTotal: c.localExpirations,
		RenewalsAcceptedTotal: c.renewalsAccepted, LeaderChangesObservedTotal: c.leaderChangesObserved,
		Store: c.store,
	}
	if m.HasObservation {
		m.ObservationAgeSeconds = max(0, now.Sub(e.observedAt).Seconds())
	}
	if m.HasSuccessfulRenewal {
		m.LastSuccessfulRenewalAgeSeconds = max(0, now.Sub(c.lastRenewalAt).Seconds())
	}
	if e.term != nil && e.leadingAtLocked(now) {
		m.IsLeader = true
		m.LeaseRemainingSeconds = e.term.deadline.Add(-e.cfg.SafetyMargin).Sub(now).Seconds()
	}
	if work := e.leaderWork; work != nil {
		m.LeaderWorkActive = true
		m.LeaderWorkCanceled = work.ctx.Err() != nil
	}
	return m
}

func (e *Elector) startOperation(m *OperationMetrics) time.Time {
	now := e.clock.Now()
	e.mu.Lock()
	m.InFlight++
	e.mu.Unlock()
	return now
}
func (e *Elector) finishOperation(m *OperationMetrics, started time.Time, err error) {
	elapsed := max(0, e.clock.Now().Sub(started).Seconds())
	// errors.Is can invoke a Store error's custom Is/Unwrap methods. Do not
	// execute application code while holding the watchdog's mutex.
	timeout := errors.Is(err, context.DeadlineExceeded)
	canceled := !timeout && errors.Is(err, context.Canceled)
	expected := !timeout && !canceled && (errors.Is(err, ErrNoLease) || errors.Is(err, ErrLeaseLost))
	e.mu.Lock()
	defer e.mu.Unlock()
	m.InFlight--
	m.CallsTotal++
	m.DurationSecondsTotal += elapsed
	m.LastDurationSeconds = elapsed
	switch {
	case err == nil:
	case timeout:
		m.ErrorsTotal++
		m.TimeoutsTotal++
	case canceled:
		m.CanceledTotal++
	case expected:
		m.ExpectedOutcomesTotal++
	default:
		m.ErrorsTotal++
	}
}
