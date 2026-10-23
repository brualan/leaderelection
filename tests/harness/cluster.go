package harness

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/brualan/leaderelection"
)

// Timings used by the chaos scenarios. They are an order of magnitude tighter
// than the library's defaults so that a scenario finishes in seconds, and they
// keep the same proportions, so a violation these expose would exist at the
// defaults too -- it would just take ten times as long to find.
const (
	LeaseDuration    = 3 * time.Second
	RenewInterval    = 900 * time.Millisecond
	RetryPeriod      = 500 * time.Millisecond
	OperationTimeout = 700 * time.Millisecond
	SafetyMargin     = 400 * time.Millisecond
)

// Cluster is a set of participants contending for one election, each reaching
// the database over its own proxy so that faults can be aimed at individuals.
type Cluster struct {
	t        *testing.T
	Election string
	Observer *Observer
	Nodes    []*Node

	tp     *Toxiproxy
	ctx    context.Context
	opts   Options
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// nodesMu guards the Nodes slice against [Cluster.Replace], which is the
	// only thing that ever writes it after the cluster is running. Scenarios
	// read Nodes directly from the test goroutine, which is also the goroutine
	// that replaces participants; the lock is for the background samplers a
	// soak runs alongside them.
	nodesMu sync.RWMutex

	// generation distinguishes a replacement participant from the one it
	// replaced. Identities must be unique across restarts: a process that
	// reuses one can renew a lease its own stalled predecessor still believes
	// it holds.
	generation int
}

// Node is one participant.
type Node struct {
	Name     string
	Identity string
	Proxy    *Proxy
	Elector  *leaderelection.Elector
	Store    Store

	cancel context.CancelFunc
	done   chan error
}

// Options configures a [Cluster].
type Options struct {
	// Nodes is how many participants contend. Three is the smallest number
	// where a partition can leave a majority behind, though this election has
	// no notion of majority -- the database is the only authority.
	Nodes int

	// Backend is the database the participants share. It defaults to
	// [PostgresBackend], and it is what decides which store, which DSN and
	// which compose services a scenario is talking about.
	Backend *Backend

	// Upstream is the address the proxies forward to, as seen from inside the
	// toxiproxy container: "postgres:5432" or "pgbouncer:5432". It defaults to
	// the backend's direct address.
	Upstream string

	// BasePort is the first published proxy port; each node takes the next.
	BasePort int

	// DSNTemplate overrides the backend's connection string template, with %s
	// for host:port.
	DSNTemplate string

	// DisableRelease makes shutdown look like a crash rather than a handover.
	DisableRelease bool

	// LeaseDuration and SafetyMargin override the defaults above. A scenario
	// that needs a wide window between the local step-down and the lease's
	// real expiry widens SafetyMargin, which is exactly that gap.
	LeaseDuration time.Duration
	SafetyMargin  time.Duration

	// NodeStore wraps the store a participant is given. It is how a scenario
	// makes one participant see the database differently from the others --
	// through a clock that disagrees, for instance -- without changing what
	// the database actually contains.
	NodeStore func(base leaderelection.Store, index int) leaderelection.Store
}

// Environment variables the chaos suite reads.
const (
	EnvToxiproxy = "LEADERELECTION_TOXIPROXY_URL"
	EnvCompose   = "LEADERELECTION_COMPOSE_FILE"
)

// ToxiproxyURL returns the control API address, defaulting to the one docker
// compose publishes.
func ToxiproxyURL() string {
	if v := os.Getenv(EnvToxiproxy); v != "" {
		return v
	}
	return "http://127.0.0.1:8474"
}

// ComposeFile returns the path to the compose project.
func ComposeFile() string {
	if v := os.Getenv(EnvCompose); v != "" {
		return v
	}
	return "../docker-compose.yml"
}

// NewCluster builds and starts a cluster. Every node is running by the time it
// returns, though none is necessarily leader yet.
func NewCluster(t *testing.T, opts Options) *Cluster {
	t.Helper()

	if opts.Backend == nil {
		opts.Backend = PostgresBackend
	}
	if opts.DSNTemplate == "" {
		opts.DSNTemplate = opts.Backend.DSNTemplate
	}
	if opts.Upstream == "" {
		opts.Upstream = opts.Backend.Direct
	}
	if opts.LeaseDuration == 0 {
		opts.LeaseDuration = LeaseDuration
	}
	if opts.SafetyMargin == 0 {
		opts.SafetyMargin = SafetyMargin
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Cluster{
		t:        t,
		Election: fmt.Sprintf("chaos-%s-%d", sanitise(t.Name()), time.Now().UnixNano()),
		Observer: NewObserver(),
		tp:       NewToxiproxy(t, ToxiproxyURL()),
		ctx:      ctx,
		opts:     opts,
		cancel:   cancel,
	}
	t.Cleanup(c.Stop)

	// The schema is created once over a direct connection, so that a scenario
	// which breaks every proxy still starts from a working table.
	c.ensureSchema(t, opts)

	for i := range opts.Nodes {
		c.Nodes = append(c.Nodes, c.startNode(ctx, opts, i))
	}
	return c
}

func (c *Cluster) ensureSchema(t *testing.T, opts Options) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := fmt.Sprintf(opts.DSNTemplate, opts.Backend.Admin)
	store, err := opts.Backend.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for schema setup: %v", err)
	}
	defer store.Close()
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
}

func (c *Cluster) startNode(ctx context.Context, opts Options, i int) *Node {
	c.t.Helper()

	name := fmt.Sprintf("node-%d", i)
	// Fixture administration has its own timeout and must work independently of node cancellation.
	proxy := c.tp.CreateProxy(c.t, //nolint:contextcheck // Fixture administration has its own timeout independent of node cancellation.
		fmt.Sprintf("%s-%s", c.Election, name), opts.BasePort+i, opts.Upstream)
	proxy.ClearToxics(c.t) //nolint:contextcheck // Fixture administration uses its own bounded context.

	store, err := opts.Backend.Open(ctx, fmt.Sprintf(opts.DSNTemplate, proxy.Listen))
	if err != nil {
		c.t.Fatalf("%s: connect: %v", name, err)
	}

	// The generation is part of the identity, so a replacement never presents
	// the one its predecessor used.
	identity := fmt.Sprintf("%s-%s-%d", c.Election, name, c.generation)
	node := &Node{
		Name:     name,
		Identity: identity,
		Proxy:    proxy,
		Store:    store,
		done:     make(chan error, 1),
	}

	var backing leaderelection.Store = store
	if opts.NodeStore != nil {
		backing = opts.NodeStore(store, i)
	}

	elector, err := leaderelection.New(leaderelection.Config{
		Store:            backing,
		Name:             c.Election,
		Identity:         identity,
		LeaseDuration:    opts.LeaseDuration,
		RenewInterval:    RenewInterval,
		RetryPeriod:      RetryPeriod,
		OperationTimeout: OperationTimeout,
		SafetyMargin:     opts.SafetyMargin,
		DisableRelease:   opts.DisableRelease,
		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(leaderCtx context.Context, fence int64) {
				// The interval is recorded as early as it can begin and closed
				// as late as it can end, so any overlap that exists is seen.
				c.Observer.Began(name, fence)
				<-leaderCtx.Done()
				c.Observer.Ended(name, fence)
			},
		},
	})
	if err != nil {
		c.t.Fatalf("%s: new elector: %v", name, err)
	}
	node.Elector = elector

	nodeCtx, cancelNode := context.WithCancel(ctx)
	node.cancel = cancelNode

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		node.done <- elector.Run(nodeCtx)
	}()
	return node
}

// Stop shuts every node down and closes its pool.
func (c *Cluster) Stop() {
	c.cancel()
	c.wg.Wait()
	for _, n := range c.Nodes {
		n.Store.Close()
	}
}

// Kill stops one node without letting it release the lease, the way a process
// killed with SIGKILL leaves the election.
func (c *Cluster) Kill(t *testing.T, node *Node) {
	t.Helper()
	// Severing the path first means the release cannot reach the database even
	// if the node tries, which is what makes this a crash rather than an exit.
	node.Proxy.Cut(t)
	node.cancel()
	select {
	case err := <-node.done:
		if err != nil {
			t.Fatalf("%s stopped with error: %v", node.Name, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not stop", node.Name)
	}
}

// Shutdown stops one node the way a rolling deployment does: its context is
// cancelled, it finishes its leader work, and it releases the lease so that a
// successor need not wait out the expiry.
func (c *Cluster) Shutdown(t *testing.T, node *Node) {
	t.Helper()
	node.cancel()
	select {
	case err := <-node.done:
		if err != nil {
			t.Fatalf("%s: Run returned %v", node.Name, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not stop", node.Name)
	}
}

// Replace stops a participant without letting it release, and starts a fresh
// one in its place -- a new identity, a new pool, the same slot. This is what
// a rolling deployment does, one pod at a time.
func (c *Cluster) Replace(t *testing.T, node *Node) *Node {
	t.Helper()

	index := -1
	for i, n := range c.Nodes {
		if n == node {
			index = i
		}
	}
	if index < 0 {
		t.Fatalf("%s is not part of this cluster", node.Name)
	}

	c.Kill(t, node)
	node.Store.Close()

	c.generation++
	replacement := c.startNode(c.ctx, c.opts, index)

	c.nodesMu.Lock()
	c.Nodes[index] = replacement
	c.nodesMu.Unlock()
	return replacement
}

// nodes returns the current participants. Everything that may run while a
// scenario is replacing one goes through here.
func (c *Cluster) nodes() []*Node {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()
	return append([]*Node(nil), c.Nodes...)
}

// Leader returns the node that currently considers itself leader, or nil.
// Finding two is an immediate failure: this is the invariant, sampled.
func (c *Cluster) Leader(t *testing.T) *Node {
	t.Helper()
	var found *Node
	for _, n := range c.nodes() {
		if !n.Elector.IsLeader() {
			continue
		}
		if found != nil {
			t.Fatalf("two nodes claim leadership at the same instant: %s and %s",
				found.Name, n.Name)
		}
		found = n
	}
	return found
}

// AwaitLeader blocks until some node leads, sampling the invariant while it
// waits.
func (c *Cluster) AwaitLeader(t *testing.T, within time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if leader := c.Leader(t); leader != nil {
			return leader
		}
		if time.Now().After(deadline) {
			t.Fatalf("no node became leader within %v", within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// AwaitNoLeader blocks until no node considers itself leader. This is what a
// correct response to a database outage looks like: nobody leads.
func (c *Cluster) AwaitNoLeader(t *testing.T, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if c.Leader(t) == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a node was still leading %v after it lost the database; "+
				"uncertainty must resolve to not-leader", within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// AwaitLeaderOtherThan blocks until a node other than excluded leads.
func (c *Cluster) AwaitLeaderOtherThan(t *testing.T, excluded *Node, within time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if leader := c.Leader(t); leader != nil && leader != excluded {
			return leader
		}
		if time.Now().After(deadline) {
			t.Fatalf("no node other than %s took over within %v", excluded.Name, within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Sample keeps checking the invariant for d, which catches an overlap that
// opens and closes between two explicit assertions.
func (c *Cluster) Sample(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c.Leader(t)
		time.Sleep(5 * time.Millisecond)
	}
}

func sanitise(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}

// Observer records when each participant believed it was leader.
//
// It is the safety oracle. Everything else in a chaos scenario is setup; this
// is the assertion.
type Observer struct {
	mu        sync.Mutex
	intervals []Interval
	open      map[string]int
}

// Interval is one leadership term as the participant experienced it.
type Interval struct {
	Node  string
	Fence int64
	Start time.Time
	End   time.Time
}

// NewObserver returns an empty Observer.
func NewObserver() *Observer {
	return &Observer{open: map[string]int{}}
}

// Began records the start of a term.
func (o *Observer) Began(node string, fence int64) {
	now := time.Now()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.open[key(node, fence)] = len(o.intervals)
	o.intervals = append(o.intervals, Interval{Node: node, Fence: fence, Start: now})
}

// Ended records the end of a term.
func (o *Observer) Ended(node string, fence int64) {
	now := time.Now()
	o.mu.Lock()
	defer o.mu.Unlock()
	if i, ok := o.open[key(node, fence)]; ok {
		o.intervals[i].End = now
		delete(o.open, key(node, fence))
	}
}

func key(node string, fence int64) string { return fmt.Sprintf("%s#%d", node, fence) }

// Check asserts the safety property: no two participants believed themselves
// leader at overlapping times, and fence tokens never went backwards.
//
// A term still open when Check runs is treated as running to now, which is
// correct -- the participant still believes it leads.
func (o *Observer) Check(t *testing.T) {
	t.Helper()

	o.mu.Lock()
	intervals := make([]Interval, len(o.intervals))
	copy(intervals, o.intervals)
	o.mu.Unlock()

	now := time.Now()
	for i := range intervals {
		if intervals[i].End.IsZero() {
			intervals[i].End = now
		}
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].Start.Before(intervals[j].Start) })

	if len(intervals) == 0 {
		t.Fatal("no leadership terms were observed; the scenario proved nothing")
	}

	for i := 1; i < len(intervals); i++ {
		prev, cur := intervals[i-1], intervals[i]
		if cur.Start.Before(prev.End) {
			t.Fatalf("two leaders overlapped by %v:\n  %s held fence %d from %s to %s\n  %s held fence %d from %s to %s",
				prev.End.Sub(cur.Start),
				prev.Node, prev.Fence, stamp(prev.Start), stamp(prev.End),
				cur.Node, cur.Fence, stamp(cur.Start), stamp(cur.End))
		}
		// Fence tokens are what a downstream system uses to reject a stale
		// leader's writes. If they can repeat or go backwards, that defence
		// is gone even when the intervals themselves are clean.
		if cur.Fence <= prev.Fence {
			t.Fatalf("fence went backwards across a handover: %s held %d, then %s held %d",
				prev.Node, prev.Fence, cur.Node, cur.Fence)
		}
	}

	t.Logf("safety oracle: %d leadership terms, no overlap, fences %d..%d",
		len(intervals), intervals[0].Fence, intervals[len(intervals)-1].Fence)
}

// Terms returns how many leadership terms have been observed.
func (o *Observer) Terms() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.intervals)
}

func stamp(t time.Time) string { return t.Format("15:04:05.000") }
