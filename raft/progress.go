package raft

import "sort"

// ProgressState describes how the leader is replicating to a follower.
type ProgressState uint8

const (
	// ProgressStateProbe: the leader is unsure of the follower's match index and
	// sends at most one append per heartbeat to probe it.
	ProgressStateProbe ProgressState = iota
	// ProgressStateReplicate: the leader knows the match index and streams
	// entries optimistically.
	ProgressStateReplicate
	// ProgressStateSnapshot: the follower is too far behind; the leader is
	// sending it a snapshot and pauses normal replication.
	ProgressStateSnapshot
)

func (s ProgressState) String() string {
	switch s {
	case ProgressStateProbe:
		return "probe"
	case ProgressStateReplicate:
		return "replicate"
	case ProgressStateSnapshot:
		return "snapshot"
	default:
		return "unknown"
	}
}

// Progress tracks one follower's replication state, as seen by the leader.
type Progress struct {
	Match, Next uint64
	State       ProgressState
	// RecentActive is set when the follower responded recently; CheckQuorum
	// resets it each election timeout to detect lost quorum.
	RecentActive bool
	// ProbeSent throttles probes to one per heartbeat interval.
	ProbeSent bool
	// PendingSnapshot is the index of an in-flight snapshot.
	PendingSnapshot uint64
	// IsLearner marks non-voting members.
	IsLearner bool
}

func (pr *Progress) becomeProbe() {
	if pr.State == ProgressStateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.resetState(ProgressStateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.resetState(ProgressStateProbe)
		pr.Next = pr.Match + 1
	}
}

func (pr *Progress) becomeReplicate() {
	pr.resetState(ProgressStateReplicate)
	pr.Next = pr.Match + 1
}

func (pr *Progress) becomeSnapshot(snapshotIndex uint64) {
	pr.resetState(ProgressStateSnapshot)
	pr.PendingSnapshot = snapshotIndex
}

func (pr *Progress) resetState(state ProgressState) {
	pr.ProbeSent = false
	pr.PendingSnapshot = 0
	pr.State = state
}

// maybeUpdate moves Match/Next forward on a successful append response.
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
		pr.ProbeSent = false
	}
	pr.Next = max(pr.Next, n+1)
	return updated
}

// maybeDecrTo backs Next off on a rejected append, using the follower's hint.
func (pr *Progress) maybeDecrTo(rejected, matchHint uint64) bool {
	if pr.State == ProgressStateReplicate {
		if rejected <= pr.Match {
			return false // stale rejection
		}
		pr.Next = pr.Match + 1
		return true
	}
	if pr.Next-1 != rejected {
		return false // stale rejection
	}
	pr.Next = max(min(rejected, matchHint+1), 1)
	pr.ProbeSent = false
	return true
}

// ProgressTracker holds all follower progress plus the membership config.
type ProgressTracker struct {
	Progress map[uint64]*Progress
	Votes    map[uint64]bool // votes received in the current election
}

func makeProgressTracker() ProgressTracker {
	return ProgressTracker{
		Progress: map[uint64]*Progress{},
		Votes:    map[uint64]bool{},
	}
}

// voterIDs returns the sorted IDs of voting members (deterministic order).
func (p *ProgressTracker) voterIDs() []uint64 {
	ids := make([]uint64, 0, len(p.Progress))
	for id, pr := range p.Progress {
		if !pr.IsLearner {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// allIDs returns all member IDs (voters + learners) in sorted order.
func (p *ProgressTracker) allIDs() []uint64 {
	ids := make([]uint64, 0, len(p.Progress))
	for id := range p.Progress {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (p *ProgressTracker) quorum() int {
	return len(p.voterIDs())/2 + 1
}

// committed computes the highest index replicated on a majority of voters.
func (p *ProgressTracker) committed() uint64 {
	voters := p.voterIDs()
	if len(voters) == 0 {
		return 0
	}
	matches := make([]uint64, 0, len(voters))
	for _, id := range voters {
		matches = append(matches, p.Progress[id].Match)
	}
	// Sort descending; the value at index quorum-1 is replicated on a majority.
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	return matches[p.quorum()-1]
}

// recordVote records a peer's vote.
func (p *ProgressTracker) recordVote(id uint64, granted bool) {
	if _, ok := p.Votes[id]; !ok {
		p.Votes[id] = granted
	}
}

// tallyVotes returns granted/rejected counts and whether the election is won,
// lost, or still pending.
func (p *ProgressTracker) tallyVotes() (granted, rejected int, result voteResult) {
	for _, id := range p.voterIDs() {
		v, voted := p.Votes[id]
		if !voted {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	q := p.quorum()
	switch {
	case granted >= q:
		return granted, rejected, voteWon
	case rejected >= q:
		return granted, rejected, voteLost
	default:
		return granted, rejected, votePending
	}
}

type voteResult uint8

const (
	votePending voteResult = iota
	voteLost
	voteWon
)

func (p *ProgressTracker) resetVotes() {
	p.Votes = map[uint64]bool{}
}
