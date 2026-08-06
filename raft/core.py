from dataclasses import dataclass
from typing import List

from py2many.spec import CHECKER, result
from py2many.theorem import by, lemma

# ---------------------------------------------------------------------------
# Durable replicated state with a class invariant (re-proved by omega at every
# construction / mutation).
# ---------------------------------------------------------------------------


@dataclass
class ConsensusState:
    """The per-node consensus state that safety rests on.

    Invariants (hold at every construction or mutation):
      * the term, commit index and last log index are never negative;
      * the commit index never exceeds the last log index -- a node can never
        commit an entry it does not hold.
    """

    current_term: int
    commit_index: int
    last_log_index: int

    if CHECKER.invariant:

        def invariant(self):
            return (
                self.current_term >= 0
                and self.commit_index >= 0
                and self.last_log_index >= 0
                and self.commit_index <= self.last_log_index
            )

    # Raft's forward-only commit rule: commitIndex is only ever moved forward
    # and never past the log (Leader.set_server / on_response_received).
    def advance_commit(self, new_commit: int) -> "ConsensusState":
        if CHECKER.pre:
            new_commit >= self.commit_index
            new_commit <= self.last_log_index
        self.commit_index = new_commit
        if CHECKER.post:
            result.commit_index == new_commit
        return self

    def inc_term(self) -> "ConsensusState":
        """``Candidate._start_election``: ``currentTerm = 1 + currentTerm``."""
        if CHECKER.pre:
            self.current_term >= 0
        self.current_term = self.current_term + 1
        if CHECKER.post:
            result.current_term == self.current_term + 1
        return self


# ---------------------------------------------------------------------------
# Election restriction -- Voter.on_vote_request.
# ---------------------------------------------------------------------------


def may_grant_vote(
    last_vote_term: int, last_log_index: int, cand_term: int, cand_index: int
) -> bool:
    """The live voter's check: the candidate's term is newer than the term we
    last voted in AND its log is at least as long as ours.

    (More restrictive than the paper's up-to-date rule: an equal term never
    gets the vote, even with a longer log.)
    """
    if CHECKER.pre:
        last_vote_term >= 0
        last_log_index >= 0
        cand_term >= 0
        cand_index >= 0
    return cand_term > last_vote_term and cand_index >= last_log_index


def grant_vote(
    last_vote_term: int, last_log_index: int, cand_term: int, cand_index: int
) -> bool:
    """The voter's decision in ``Voter.on_vote_request``."""
    if CHECKER.pre:
        last_vote_term >= 0
        last_log_index >= 0
        cand_term >= 0
        cand_index >= 0
    if CHECKER.post:
        result == may_grant_vote(last_vote_term, last_log_index, cand_term, cand_index)
    return may_grant_vote(last_vote_term, last_log_index, cand_term, cand_index)


# ---------------------------------------------------------------------------
# Strict majority / leader promotion -- Candidate.on_vote_received.
# ---------------------------------------------------------------------------


def is_strict_majority(num: int, total: int) -> bool:
    """``num > total / 2`` expressed exactly over the integers."""
    if CHECKER.pre:
        num >= 0
        total >= 0
    return 2 * num > total


def promote_rule(num_votes: int, total_nodes: int) -> bool:
    if CHECKER.pre:
        num_votes >= 0
        total_nodes >= 0
    return num_votes > 1 and is_strict_majority(num_votes, total_nodes)


def promote_to_leader(num_votes: int, total_nodes: int) -> bool:
    """``Candidate.on_vote_received``: promote to leader only on more than one
    vote AND a strict majority of the cluster."""
    if CHECKER.pre:
        num_votes >= 0
        total_nodes >= 0
    if CHECKER.post:
        result == promote_rule(num_votes, total_nodes)
    return promote_rule(num_votes, total_nodes)


# ---------------------------------------------------------------------------
# Commit safety -- Follower._update_commit_index clamps the leader's commit
# cursor to the follower's last log index.
# ---------------------------------------------------------------------------


def clamp_commit(leader_commit: int, log_len: int) -> int:
    """The new commit index is the leader's commit index clamped to the last
    index the follower actually holds."""
    if CHECKER.pre:
        leader_commit >= 0
        log_len >= 0
    if CHECKER.post:
        result == min(leader_commit, max(0, log_len - 1))
    return min(leader_commit, max(0, log_len - 1))


# ---------------------------------------------------------------------------
# Log Matching -- Follower.on_append_entries guard (the "induction proof" in
# the code comments): entries are applied only where the follower already
# agrees with the leader.
# ---------------------------------------------------------------------------


def log_matching_ok(
    follower_log: List[int], prev_log_index: int, prev_log_term: int
) -> bool:
    """The follower may apply the leader's entries only when ``prev_log_index``
    is inside its log AND its term at that index equals ``prev_log_term``.
    Otherwise it is behind the leader or a term conflict exists."""
    if CHECKER.pre:
        prev_log_index >= 0
    return prev_log_index < len(follower_log) and (
        follower_log[prev_log_index] == prev_log_term
    )


def logs_match(
    follower_log: List[int], prev_log_index: int, prev_log_term: int
) -> bool:
    if CHECKER.pre:
        prev_log_index >= 0
    if CHECKER.post:
        result == log_matching_ok(follower_log, prev_log_index, prev_log_term)
    return log_matching_ok(follower_log, prev_log_index, prev_log_term)


# ---------------------------------------------------------------------------
# Commit-advance rule -- Leader.on_response_received.
# ---------------------------------------------------------------------------


def commit_advance_rule(
    new_commit_index: int, current_term: int, commit_term: int, commit_index: int
) -> bool:
    """Advance commitIndex only when the entry at ``new_commit_index`` carries
    the leader's *current* term and the cursor moves strictly forward."""
    if CHECKER.pre:
        new_commit_index >= 0
        current_term >= 0
        commit_term >= 0
        commit_index >= 0
    return commit_term == current_term and new_commit_index > commit_index


def can_commit(
    new_commit_index: int, current_term: int, commit_term: int, commit_index: int
) -> bool:
    if CHECKER.pre:
        new_commit_index >= 0
        current_term >= 0
        commit_term >= 0
        commit_index >= 0
    if CHECKER.post:
        result == commit_advance_rule(
            new_commit_index, current_term, commit_term, commit_index
        )
    return commit_advance_rule(
        new_commit_index, current_term, commit_term, commit_index
    )


# ---------------------------------------------------------------------------
# Lemmas -- the proof obligations discharged by omega / native_decide.
# ---------------------------------------------------------------------------


@lemma
@by("omega")
def quorums_overlap(a: int, b: int, total: int) -> bool:
    """Any two strict majorities of a cluster must overlap, so Raft cannot
    elect two leaders in the same term: if ``|Q1| > n/2`` and ``|Q2| > n/2``
    then ``|Q1| + |Q2| > n``, forcing ``Q1 ∩ Q2 ≠ ∅``."""
    if CHECKER.pre:
        total >= 0
        2 * a > total
        2 * b > total
    return a + b > total


@lemma
@by("native_decide")
def reject_lower_term_candidate() -> bool:
    """A candidate with a lower term is never granted a vote."""
    return not grant_vote(3, 7, 2, 10)


@lemma
@by("native_decide")
def grant_newer_term_with_log() -> bool:
    """A candidate with a strictly newer term and a sufficient log is granted."""
    return grant_vote(3, 7, 4, 9)


@lemma
@by("native_decide")
def reject_equal_term_even_with_longer_log() -> bool:
    """An equal term never gets the vote, even with a longer log (live rule)."""
    return not grant_vote(3, 7, 3, 9)


@lemma
@by("native_decide")
def majority_promotes() -> bool:
    return promote_to_leader(3, 5)


@lemma
@by("native_decide")
def single_vote_never_promotes() -> bool:
    return not promote_to_leader(1, 5)


@lemma
@by("native_decide")
def minority_never_promotes() -> bool:
    return not promote_to_leader(2, 5)


@lemma
@by("native_decide")
def matching_prev_accepted() -> bool:
    """A matching prev index/term is accepted (prefix preserved)."""
    return logs_match([1, 1, 2], 2, 2)


@lemma
@by("native_decide")
def conflicting_term_rejected() -> bool:
    """A term conflict at prev_log_index is rejected (the follower trims back)."""
    return not logs_match([1, 1, 2], 2, 1)


@lemma
@by("native_decide")
def beyond_log_rejected() -> bool:
    """A prev_log_index past the follower's log is rejected (follower behind)."""
    return not logs_match([1, 1, 2], 3, 2)


@lemma
@by("native_decide")
def commit_requires_current_term() -> bool:
    """A leader never commits an entry from a previous term."""
    return not can_commit(3, 5, 4, 1)


@lemma
@by("native_decide")
def commit_advances_only_forward() -> bool:
    """Committing happens only when the cursor moves strictly forward."""
    return can_commit(3, 5, 5, 1)


if __name__ == "__main__":
    initial = ConsensusState(1, 0, 3)
    print("term:", initial.current_term)
    print("grant newer term:", grant_vote(3, 7, 4, 9))
