package leaderelection

import (
	"testing"
	"time"

	"github.com/brualan/leaderelection/internal/assert"
)

// Named scenarios built on the simulation. Each one is a claim about the
// election that is either impossible or impossibly slow to make any other way;
// the fuzz target in fuzz_test.go explores the space around them.

// The baseline. A suite whose quiet case flaps proves nothing about its loud
// ones.
func TestSimulatedSteadyStateKeepsOneLeader(t *testing.T) {
	s := newSim(t, simOptions{nodes: 3})
	s.run(30*time.Second, nil)

	leader := s.leader()
	if leader == nil {
		t.Fatal("nobody led after 30s of virtual time with nothing broken")
	}
	s.verify()
	assert.Equal(t, s.termCount(), 1, "leadership terms with nothing broken")
}

// Participants in different clusters run on different hardware, and their
// clocks do not merely disagree on the time -- they disagree on how fast time
// passes. A leader whose clock runs slow converts the database's remaining
// lease into local ticks that last longer than they should, and it holds its
// term past the moment the database considers the lease free.
//
// SafetyMargin is what pays for that. The rates here bracket the worst drift
// it can cover: with a margin of m and a lease of L, a participant is safe
// while its clock runs at no less than 1 - m/L of true time, which is 0.867
// for these numbers. 0.93 leaves room without making the test toothless.
func TestSimulatedClockRateSkewIsAbsorbedByTheSafetyMargin(t *testing.T) {
	s := newSim(t, simOptions{nodes: 3, rates: []int{930, 930, 1000}})

	// A slow clock only matters to whoever is holding a lease, so each round
	// takes the database away from the participant that currently holds one.
	// That is the moment its local deadline is the only thing between it and a
	// successor: the deadline it measured in ticks that are too long.
	for round := range 4 {
		s.run(3*simLease, nil)

		leader := s.leader()
		if leader == nil {
			t.Fatalf("round %d: nobody led", round)
		}
		s.breakPath(leader, simHung, 0, 2*simLease)
		s.run(2*simLease, nil)
		s.heal(leader)
	}

	s.settle(10 * simLease)
	s.verify()
}

// Nobody can reach the database. The correct outcome is that nobody leads --
// not that the last leader carries on because nothing has told it otherwise.
func TestSimulatedTotalOutageElectsNobody(t *testing.T) {
	s := newSim(t, simOptions{nodes: 3})
	s.run(10*time.Second, nil)
	if s.leader() == nil {
		t.Fatal("no leader before the outage")
	}

	for _, n := range s.nodes {
		s.breakPath(n, simHung, 0, 4*simLease)
	}

	// Within one lease of losing the database every participant must have
	// stepped down, and none may come back while it is still unreachable.
	deadline := 2 * simLease
	for elapsed := time.Duration(0); elapsed < deadline; elapsed += simStep {
		s.tick()
	}
	if leader := s.leader(); leader != nil {
		t.Fatalf("%s still led %v after the database stopped answering", leader.name, deadline)
	}

	s.settle(10 * simLease)
	s.verify()
}

// A statement that ran and whose answer never came back is the case the whole
// design is built around: the participant cannot tell it apart from one that
// never reached the database, and the two mean opposite things about what the
// database now contains.
func TestSimulatedLostRepliesNeverProduceTwoLeaders(t *testing.T) {
	s := newSim(t, simOptions{nodes: 3})
	s.run(5*time.Second, nil)

	for round := range 6 {
		s.breakPath(s.nodes[round%len(s.nodes)], simLostReply, 100*time.Millisecond, simLease)
		s.run(simLease+simRenew, nil)
	}

	s.settle(10 * simLease)
	s.verify()
}

// The database's clock is corrected backwards, which makes every lease it
// reports look longer than the one that was actually granted. A participant
// that believed the inflated number would hold its term past the point where
// the database is entitled to give it away.
func TestSimulatedBackwardsDatabaseClockCannotExtendATerm(t *testing.T) {
	s := newSim(t, simOptions{nodes: 3})
	s.run(10*time.Second, nil)

	// The deadline bound is checked on every tick by the oracle; stepping the
	// clock repeatedly is what gives it something to catch.
	for range 5 {
		s.store.advance(-2 * simLease)
		s.run(2*simLease, nil)
	}

	s.settle(10 * simLease)
	s.verify()
}
