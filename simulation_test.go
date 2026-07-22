package leaderelection

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brualan/leaderelection/internal/clock"
)

// A whole election -- several participants, one shared record, and a database
// path that can be broken per participant -- running in one process on virtual
// time.
//
// The chaos suite in ./tests proves the same property against a real
// PostgreSQL, and it is the more convincing of the two. It is also slow, needs
// Docker, and explores interleavings only as fast as the wall clock allows. A
// simulation gives up the realism and buys back three things worth having:
//
//   - a run of several virtual minutes costs milliseconds, so a fuzzer can
//     explore thousands of fault schedules;
//   - participants' clocks can be made to run at *different rates*, which is
//     what SafetyMargin exists to absorb and which nothing outside a
//     simulation can arrange;
//   - the oracle is sampled at every step of virtual time rather than every
//     few milliseconds of real time.
//
// # What is and is not the store's fault
//
// The adversary never breaks the [Store] contract. It delays replies, loses
// them, hangs forever and fails outright -- but the record it guards is always
// a single source of truth with a single holder, exactly as the contract
// requires. No election can be safe against a store that hands one lease to
// two participants, and pretending otherwise would only manufacture failures
// the library was never asked to survive.
//
// The database's clock steps backwards here but never forwards, for the same
// reason. The database is the authority on time. An authority that jumps
// forwards retires a live lease early, and no participant reading that clock
// can know it happened -- the incumbent's local deadline still says it holds
// what the database has already given away. Backwards is safe by construction:
// it makes every lease look longer to everyone at once, and the elector caps
// what it will believe at the duration it asked for.
//
// # Determinism
//
// Virtual time is deterministic; the Go scheduler is not. A failing fuzz input
// reproduces in the great majority of cases but not in all of them -- re-run a
// reproducer with -count if it passes the first time. The journal printed on
// failure is what makes a schedule this long readable when it does.

// Simulated timings. They keep the proportions of the library's defaults while
// being small enough that minutes of virtual time pass in milliseconds, and
// they are the numbers the chaos suite uses, so a violation found here is a
// violation there.
const (
	simLease   = 3 * time.Second
	simRenew   = 900 * time.Millisecond
	simRetry   = 500 * time.Millisecond
	simOpLimit = 700 * time.Millisecond
	simMargin  = 400 * time.Millisecond

	// simStep is how far virtual time moves between two samples of the oracle.
	// At a fortieth of the safety margin, an overlap cannot open and close
	// unseen between two samples.
	simStep = 10 * time.Millisecond

	// simFaultEvery is how often the schedule gets to change the world.
	simFaultEvery = 20 * simStep
)

// The simulated clocks start three days apart, because a participant and a
// database in this library's world share nothing, least of all a clock.
var (
	simEpoch   = time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	simDBEpoch = simEpoch.Add(72 * time.Hour)
)

var (
	errSimUnreachable = errors.New("simulated: database unreachable")
	errSimReplyLost   = errors.New("simulated: connection lost after the statement ran")
)

// sim is one simulated election.
type sim struct {
	t     *testing.T
	store *simStore
	nodes []*simNode

	mu      sync.Mutex
	trueNow time.Duration // virtual time since the start of the run
	terms   []simTerm
	journal []string

	stopped bool
}

// simTerm is one leadership term as the oracle saw it.
type simTerm struct {
	node  string
	fence int64
	start time.Duration
	end   time.Duration
}

// simNode is one participant: an Elector, its own clock, and the state of the
// path between it and the database.
type simNode struct {
	name     string
	identity string
	clk      *clock.Fake
	elector  *Elector

	// rate is this participant's clock speed in parts per thousand of true
	// time. 1000 keeps perfect time with the database.
	rate int

	cancel context.CancelFunc
	done   chan error
	exited bool

	sim *sim

	// Guarded by sim.mu.
	fault simFault
}

// simFault is what the path between one participant and the database is doing.
type simFault struct {
	kind  simFaultKind
	delay time.Duration
	until time.Duration // in sim.trueNow terms
}

type simFaultKind int

const (
	// simHealthy is a working path.
	simHealthy simFaultKind = iota
	// simSlow delivers the reply late. Late enough and the operation times out
	// on its own, which is the interesting half of this fault.
	simSlow
	// simHung accepts the request and never answers: a paused server, a
	// blackholed route, a pooler that swallowed the query. The watchdog is the
	// only thing that can end a term under this one.
	simHung
	// simRefused fails immediately without reaching the database, the way a
	// closed pool or a refused connection does.
	simRefused
	// simLostReply runs the statement and loses the answer. A participant
	// cannot tell this apart from simRefused, and the two mean opposite things
	// about what the database now contains -- which is exactly why an unknown
	// outcome has to resolve to "not leader".
	simLostReply
	// simClockStep moves the database's clock backwards. See the note above on
	// why it never moves forwards.
	simClockStep

	simFaultKinds = int(simClockStep) + 1
)

func (k simFaultKind) String() string {
	switch k {
	case simHealthy:
		return "healthy"
	case simSlow:
		return "slow"
	case simHung:
		return "hung"
	case simRefused:
		return "refused"
	case simLostReply:
		return "reply-lost"
	default:
		return "clock-step"
	}
}

// simOptions configures a simulation.
type simOptions struct {
	nodes int
	// rates are the participants' clock speeds in parts per thousand. A
	// missing or zero entry means 1000.
	rates []int
}

// newSim builds and starts a simulated election. Every participant is running
// by the time it returns.
func newSim(t *testing.T, opts simOptions) *sim {
	t.Helper()

	if opts.nodes == 0 {
		opts.nodes = 3
	}

	s := &sim{t: t}
	s.store = &simStore{now: simDBEpoch, records: map[string]*simRecord{}}
	for i := range opts.nodes {
		s.nodes = append(s.nodes, s.start(i, opts))
	}
	t.Cleanup(s.stop)
	return s
}

func (s *sim) start(i int, opts simOptions) *simNode {
	s.t.Helper()

	rate := 1000
	if i < len(opts.rates) && opts.rates[i] != 0 {
		rate = opts.rates[i]
	}
	n := &simNode{
		name:     fmt.Sprintf("node-%d", i),
		identity: fmt.Sprintf("sim-node-%d", i),
		clk:      clock.NewFake(simEpoch),
		rate:     rate,
		done:     make(chan error, 1),
		sim:      s,
	}

	elector, err := New(Config{
		// Each participant reaches the shared record over its own connection,
		// which is what makes a fault aimed at one of them possible.
		Store:            &simConn{store: s.store, node: n},
		Name:             "sim-election",
		Identity:         n.identity,
		LeaseDuration:    simLease,
		RenewInterval:    simRenew,
		RetryPeriod:      simRetry,
		OperationTimeout: simOpLimit,
		SafetyMargin:     simMargin,
		clock:            n.clk,
		// Jitter is drawn from a sequence of this participant's own, so that no
		// two of them poll in lockstep and no run depends on the global source.
		randFloat: simRand(uint64(i)*2654435761 + 1),
		Callbacks: Callbacks{
			OnStartedLeading: func(ctx context.Context, fence int64) {
				s.began(n, fence)
				<-ctx.Done()
				s.ended(n, fence)
			},
		},
	})
	if err != nil {
		s.t.Fatalf("%s: New: %v", n.name, err)
	}
	n.elector = elector

	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	go func() { n.done <- elector.Run(ctx) }()
	return n
}

// tick advances virtual time by one step and samples the oracle.
func (s *sim) tick() {
	s.awaitParked()

	s.mu.Lock()
	s.trueNow += simStep
	s.mu.Unlock()

	s.advanceClocks()
	// Let whatever those ticks woke actually run before deciding what is true.
	for range 3 {
		runtime.Gosched()
	}
	s.check()
}

// awaitParked waits until every participant is waiting on its clock rather
// than merely runnable.
//
// Without this the simulation runs away from the code it is simulating.
// Virtual time only stands still while a timer is armed, so advancing it past
// a participant that has not yet armed its next one lets that participant come
// back to find seconds gone that it never got a chance to react to. What it
// does next is then a fact about how busy the test machine was, which is not a
// property this library can be held to.
func (s *sim) awaitParked() {
	deadline := time.Now().Add(30 * time.Second)
	for !s.parked() {
		if time.Now().After(deadline) {
			s.fatalf("a participant stopped waiting on its clock altogether")
		}
		runtime.Gosched()
	}
}

func (s *sim) parked() bool {
	for _, n := range s.nodes {
		if n.clk.Timers() == 0 {
			return false
		}
	}
	return true
}

func (s *sim) advanceClocks() {
	s.store.advance(simStep)
	for _, n := range s.nodes {
		// A participant whose clock runs slow experiences a shorter step than
		// the database does. That difference is the entire point of the rate.
		n.clk.Advance(simStep * time.Duration(n.rate) / 1000)
	}
}

// run advances virtual time by d, letting script change the world as it goes.
func (s *sim) run(d time.Duration, script *simScript) {
	for elapsed := time.Duration(0); elapsed < d; elapsed += simStep {
		if script != nil && elapsed%simFaultEvery == 0 {
			s.inject(script)
		}
		s.tick()
	}
}

// settle clears every fault and runs until somebody leads.
//
// This is the liveness half of the property, and it is not a formality: an
// election that survives a storm by never electing anyone again has not
// survived it. A deadlock, a permanently spent fence or a backoff that grew
// without bound all look like safety right up until this check.
func (s *sim) settle(within time.Duration) {
	s.mu.Lock()
	for _, n := range s.nodes {
		n.fault = simFault{}
	}
	s.mu.Unlock()

	for elapsed := time.Duration(0); elapsed < within; elapsed += simStep {
		s.tick()
		// Waiting for the term to be *recorded* rather than merely for
		// IsLeader: promotion happens before OnStartedLeading is scheduled, and
		// returning in between would leave the oracle without the term it is
		// about to be asked to check.
		if leader := s.leader(); leader != nil {
			if fence, ok := leader.elector.Fence(); ok && s.recorded(leader.name, fence) {
				return
			}
		}
	}
	s.fatalf("nobody led within %v of the last fault clearing", within)
}

// recorded reports whether a term has reached the oracle.
func (s *sim) recorded(node string, fence int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, term := range s.terms {
		if term.node == node && term.fence == fence {
			return true
		}
	}
	return false
}

// inject reads the next instruction from the schedule and applies it.
func (s *sim) inject(script *simScript) {
	if script.exhausted() {
		return
	}

	node := s.nodes[script.next(len(s.nodes))]
	kind := simFaultKind(script.next(simFaultKinds))

	if kind == simClockStep {
		// Backwards only, by up to two lease durations. This is the path where
		// a lease looks longer than it was ever granted for, which the elector
		// is required to disbelieve.
		back := time.Duration(script.next(256)) * 24 * time.Millisecond
		s.store.advance(-back)
		s.note("database clock stepped back by %v", back)
		return
	}

	// Up to two and a half lease durations, so that a fault can outlive a term
	// and force a real handover rather than a hiccup.
	duration := time.Duration(1+script.next(256)) * 3 * simStep
	// Up to nearly three times the operation timeout, so that a slow path is
	// sometimes survivable and sometimes not. Anything less and the timeout
	// would never fire, which is half of what this fault is for.
	delay := time.Duration(script.next(256)) * 8 * time.Millisecond

	s.mu.Lock()
	node.fault = simFault{kind: kind, delay: delay, until: s.trueNow + duration}
	s.appendJournal("%s path is %s for %v (delay %v)", node.name, kind, duration, delay)
	s.mu.Unlock()
}

// breakPath puts a fault on one participant's path for d of virtual time.
func (s *sim) breakPath(n *simNode, kind simFaultKind, delay, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n.fault = simFault{kind: kind, delay: delay, until: s.trueNow + d}
	s.appendJournal("%s path is %s for %v", n.name, kind, d)
}

// heal clears the fault on one participant's path.
func (s *sim) heal(n *simNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n.fault = simFault{}
	s.appendJournal("%s path is healthy", n.name)
}

// check is the safety oracle, sampled at every step of virtual time.
func (s *sim) check() {
	var leaders []string
	for _, n := range s.nodes {
		if !n.elector.IsLeader() {
			continue
		}
		leaders = append(leaders, n.name)

		// A term may never be valid for longer than the lease it was granted.
		// The database's clock can step backwards underneath a participant,
		// which inflates the remaining time every lease appears to have; a
		// deadline computed from that inflated number would sit past the real
		// expiry, and past the point where somebody else may take over.
		if limit := n.clk.Now().Add(simLease - simMargin); n.elector.Status().ValidUntil.After(limit) {
			s.fatalf("%s holds a term valid until %v, past the %v its lease could possibly allow",
				n.name, n.elector.Status().ValidUntil, limit)
		}
	}
	if len(leaders) > 1 {
		s.fatalf("two participants believed they were leaders at the same instant: %s",
			strings.Join(leaders, ", "))
	}
}

// terms returns how many leadership terms the oracle has recorded.
func (s *sim) termCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.terms)
}

// leader returns the participant that currently leads, or nil.
func (s *sim) leader() *simNode {
	for _, n := range s.nodes {
		if n.elector.IsLeader() {
			return n
		}
	}
	return nil
}

// verify asserts what must hold of the run as a whole once it is over.
func (s *sim) verify() {
	s.t.Helper()

	s.mu.Lock()
	terms := append([]simTerm(nil), s.terms...)
	s.mu.Unlock()

	if len(terms) == 0 {
		s.fatalf("no leadership term was ever observed; the run proved nothing")
	}

	// Fence tokens are what a downstream system uses to reject a stalled
	// leader's writes. Two terms sharing one -- same participant or not, in
	// either order -- takes that defence away. Uniqueness is asserted globally
	// rather than in the order terms were recorded, because the order a
	// callback goroutine is scheduled in is not evidence of anything.
	seen := map[int64]simTerm{}
	for _, term := range terms {
		if previous, ok := seen[term.fence]; ok {
			s.fatalf("fence %d was handed to two terms: %s at %v, %s at %v",
				term.fence, previous.node, previous.start, term.node, term.start)
		}
		seen[term.fence] = term
	}
	s.t.Logf("%d terms over %v of virtual time, fences all distinct", len(terms), s.trueNow)
}

// stop shuts every participant down, keeping virtual time moving so that the
// release each of them attempts on the way out can complete.
func (s *sim) stop() {
	s.mu.Lock()
	already := s.stopped
	s.stopped = true
	s.mu.Unlock()
	if already {
		return
	}

	for _, n := range s.nodes {
		n.cancel()
	}

	deadline := time.Now().Add(30 * time.Second)
	for !s.allExited() {
		if time.Now().After(deadline) {
			s.t.Fatal("a participant did not stop after its context was cancelled")
		}
		s.advanceClocks()
		runtime.Gosched()
	}
}

func (s *sim) allExited() bool {
	done := true
	for _, n := range s.nodes {
		if n.exited {
			continue
		}
		select {
		case err := <-n.done:
			if err != nil {
				s.t.Errorf("%s: Run returned %v", n.name, err)
			}
			n.exited = true
		default:
			done = false
		}
	}
	return done
}

func (s *sim) began(n *simNode, fence int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terms = append(s.terms, simTerm{node: n.name, fence: fence, start: s.trueNow})
	s.appendJournal("%s began a term on fence %d", n.name, fence)
}

func (s *sim) ended(n *simNode, fence int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.terms {
		if s.terms[i].node == n.name && s.terms[i].fence == fence {
			s.terms[i].end = s.trueNow
		}
	}
	s.appendJournal("%s ended its term on fence %d", n.name, fence)
}

func (s *sim) note(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendJournal(format, args...)
}

// appendJournal records one event. Callers hold s.mu.
//
// The journal is bounded: a long schedule produces thousands of events, and
// only the ones near the failure explain it.
func (s *sim) appendJournal(format string, args ...any) {
	const keep = 120
	s.journal = append(s.journal, fmt.Sprintf("%10v  ", s.trueNow)+fmt.Sprintf(format, args...))
	if len(s.journal) > 2*keep {
		s.journal = append(s.journal[:0], s.journal[len(s.journal)-keep:]...)
	}
}

// fatalf fails the test with the tail of the journal attached, which is the
// only thing that makes a fault schedule this long readable.
func (s *sim) fatalf(format string, args ...any) {
	s.mu.Lock()
	journal := strings.Join(s.journal, "\n  ")
	now := s.trueNow
	s.mu.Unlock()

	s.t.Fatalf("at %v: %s\n\nlast events:\n  %s", now, fmt.Sprintf(format, args...), journal)
}

// wait blocks for d on this participant's clock, reporting false if the
// operation's context ended first.
func (n *simNode) wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := n.clk.NewTimer(d)
	// Stopping releases an Advance parked on delivering to this timer.
	defer timer.Stop()
	select {
	case <-timer.C():
		return true
	case <-ctx.Done():
		return false
	}
}

// currentFault returns the fault on this participant's path, expiring one
// whose time is up.
func (n *simNode) currentFault() simFault {
	n.sim.mu.Lock()
	defer n.sim.mu.Unlock()
	if n.sim.trueNow >= n.fault.until {
		n.fault = simFault{}
	}
	return n.fault
}

// simConn is one participant's connection to the shared record. It is where
// the adversary lives: everything that can happen to a request on its way to
// the database, and to the answer on its way back.
type simConn struct {
	store *simStore
	node  *simNode
}

func (c *simConn) Acquire(ctx context.Context, name, identity string, ttl time.Duration, payload []byte) (Lease, error) {
	return c.roundTrip(ctx, func(now time.Time) (Lease, error) {
		return c.store.acquire(now, name, identity, ttl, payload), nil
	})
}

func (c *simConn) Renew(ctx context.Context, name, identity string, fence int64, ttl time.Duration, payload []byte) (Lease, error) {
	return c.roundTrip(ctx, func(now time.Time) (Lease, error) {
		return c.store.renew(now, name, identity, fence, ttl, payload)
	})
}

func (c *simConn) Release(ctx context.Context, name, identity string, fence int64) error {
	_, err := c.roundTrip(ctx, func(now time.Time) (Lease, error) {
		c.store.release(now, name, identity, fence)
		return Lease{}, nil
	})
	return err
}

func (c *simConn) Inspect(ctx context.Context, name string) (Lease, error) {
	return c.roundTrip(ctx, func(now time.Time) (Lease, error) {
		return c.store.inspect(now, name)
	})
}

func (c *simConn) roundTrip(ctx context.Context, apply func(time.Time) (Lease, error)) (Lease, error) {
	switch f := c.node.currentFault(); f.kind {
	case simRefused:
		// Nothing reached the database, and the participant knows only that it
		// failed.
		return Lease{}, errSimUnreachable

	case simHung:
		// The request is accepted and never answered. The operation timeout
		// ends this call; ending the term it belongs to is the watchdog's job,
		// and nothing here can help with it.
		<-ctx.Done()
		return Lease{}, context.Cause(ctx)

	case simLostReply:
		// The statement runs. The answer does not come back. Whatever the
		// database now holds, this participant will not learn it here.
		_, _ = c.store.execute(apply)
		if !c.node.wait(ctx, f.delay) {
			return Lease{}, context.Cause(ctx)
		}
		return Lease{}, errSimReplyLost

	case simSlow:
		if !c.node.wait(ctx, f.delay) {
			return Lease{}, context.Cause(ctx)
		}
	}
	return c.store.execute(apply)
}

// simStore is the shared record every participant contends for.
//
// Its semantics are the ones the acceptance suite holds every real store to,
// and they are deliberately memstore's: a single holder, a fence that advances
// on every reclaim, a renewal guarded by identity, fence and expiry together.
type simStore struct {
	mu      sync.Mutex
	now     time.Time
	records map[string]*simRecord
}

type simRecord struct {
	holder     string
	fence      int64
	acquiredAt time.Time
	renewedAt  time.Time
	expiresAt  time.Time
	payload    []byte
}

// advance moves the database's clock. A negative d is an NTP correction
// backwards, which is what makes every lease look longer than it is.
func (st *simStore) advance(d time.Duration) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.now = st.now.Add(d)
}

// execute applies one operation against the record under the store's own lock.
//
// No fault ever waits inside it: a lock held across virtual time would stall
// every other participant for a reason no real database has.
func (st *simStore) execute(apply func(time.Time) (Lease, error)) (Lease, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return apply(st.now)
}

func (st *simStore) acquire(now time.Time, name, identity string, ttl time.Duration, payload []byte) Lease {
	r, ok := st.records[name]
	if !ok {
		r = &simRecord{holder: identity, fence: 1, acquiredAt: now}
		st.records[name] = r
		r.renew(now, ttl, payload)
		return r.lease(name, now)
	}
	expired := !r.expiresAt.After(now)
	if expired || r.holder == identity {
		// A reclaimed lease starts a new term even for its previous holder:
		// while it was expired, anyone could have led.
		if expired {
			r.fence++
			r.acquiredAt = now
		}
		r.holder = identity
		r.renew(now, ttl, payload)
	}
	return r.lease(name, now)
}

func (st *simStore) renew(now time.Time, name, identity string, fence int64, ttl time.Duration, payload []byte) (Lease, error) {
	r, ok := st.records[name]
	if !ok || r.holder != identity || r.fence != fence || !r.expiresAt.After(now) {
		return Lease{}, ErrLeaseLost
	}
	r.renew(now, ttl, payload)
	return r.lease(name, now), nil
}

func (st *simStore) release(now time.Time, name, identity string, fence int64) {
	r, ok := st.records[name]
	if !ok || r.holder != identity || r.fence != fence || !r.expiresAt.After(now) {
		return
	}
	// Expired in place rather than deleted: the fence has to keep climbing
	// across the handover, and this record is what remembers it.
	r.expiresAt = now
}

func (st *simStore) inspect(now time.Time, name string) (Lease, error) {
	r, ok := st.records[name]
	if !ok {
		return Lease{}, ErrNoLease
	}
	return r.lease(name, now), nil
}

func (r *simRecord) renew(now time.Time, ttl time.Duration, payload []byte) {
	r.renewedAt = now
	r.expiresAt = now.Add(ttl)
	r.payload = append([]byte(nil), payload...)
}

func (r *simRecord) lease(name string, now time.Time) Lease {
	return Lease{
		Name:       name,
		Holder:     r.holder,
		Fence:      r.fence,
		Payload:    append([]byte(nil), r.payload...),
		AcquiredAt: r.acquiredAt,
		RenewedAt:  r.renewedAt,
		ExpiresAt:  r.expiresAt,
		Now:        now,
	}
}

// simScript turns fuzz input into a fault schedule.
//
// Running out of bytes means "no more faults", which gives every run a
// quiescent tail for free -- and that tail is what makes the liveness
// assertion meaningful.
type simScript struct {
	data []byte
	pos  int
}

func (s *simScript) exhausted() bool { return s.pos >= len(s.data) }

// next returns a value in [0, n), consuming one byte. n above 256 would make
// the top of the range unreachable, so callers that want a wider spread scale
// the result instead of asking for a wider draw.
func (s *simScript) next(n int) int {
	if n <= 0 || s.exhausted() {
		return 0
	}
	v := int(s.data[s.pos]) % n
	s.pos++
	return v
}

// simRand returns a deterministic source of values in [0, 1), so that a run
// depends on its input and on nothing else.
func simRand(seed uint64) func() float64 {
	state := seed | 1
	var mu sync.Mutex
	return func() float64 {
		mu.Lock()
		defer mu.Unlock()
		// xorshift64*, which is plenty for smearing poll intervals.
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		return float64((state*2685821657736338717)>>11) / (1 << 53)
	}
}
