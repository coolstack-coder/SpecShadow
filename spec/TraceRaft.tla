----------------------------- MODULE TraceRaft -----------------------------
(***************************************************************************)
(* Trace validation for AegisDB's raft.                                    *)
(*                                                                         *)
(* The implementation records one event per state change, with the node's *)
(* state right after it. This spec replays those events in order. For     *)
(* every step it asks two questions:                                      *)
(*   1. Was this step allowed by raft's rules, given everything that       *)
(*      happened before it? (vote once per term, leaders need a quorum,   *)
(*      only commit current-term entries by counting, ...)                *)
(*   2. Do the global safety properties still hold? (one leader per term, *)
(*      committed entries never change, every node applies the same      *)
(*      entries, reads are linearizable, ...)                            *)
(* The first failed check is stored in err, and the invariant Ok makes    *)
(* TLC stop at exactly that step.                                         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC, TraceData

(* TraceData defines:
     Nodes  the node ids
     Trace  a sequence of event records, see checker/tla.go          *)

Quorum   == Cardinality(Nodes) \div 2 + 1
Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b
LastTerm(s) == IF s = <<>> THEN 0 ELSE s[Len(s)]
Prefix(s, k) == SubSeq(s, 1, Min(k, Len(s)))

VARIABLES
    i,        \* index of the next event to replay
    err,      \* "ok", or the name of the first rule that was broken
    term, role, vote, log, commit,   \* each node's state, as the trace says
    up,       \* is the process running
    grants,   \* every vote ever granted: <<voter, candidate, term>>
    leaders,  \* every election won: <<node, term>>
    llog,     \* llog[n] = <<t, l>>: the longest log n had while leading term t
    dur,      \* dur[n]: the log prefix n has acknowledged, so must have on disk
    clog,     \* the committed log, as announced by leaders
    cterm,    \* cterm[k]: term of the leader that first committed index k
    applied,  \* last index each node applied to its state machine
    acked,    \* highest write index acknowledged to a client so far
    reads     \* reads[rid]: acked when read rid started

vars == <<i, err, term, role, vote, log, commit, up, grants, leaders, llog,
          dur, clog, cterm, applied, acked, reads>>

Init ==
    /\ i = 1
    /\ err = "ok"
    /\ term = [n \in Nodes |-> 0]
    /\ role = [n \in Nodes |-> "F"]
    /\ vote = [n \in Nodes |-> ""]
    /\ log = [n \in Nodes |-> <<>>]
    /\ commit = [n \in Nodes |-> 0]
    /\ up = [n \in Nodes |-> TRUE]
    /\ grants = {}
    /\ leaders = {}
    /\ llog = [n \in Nodes |-> <<0, <<>>>>]
    /\ dur = [n \in Nodes |-> <<>>]
    /\ clog = <<>>
    /\ cterm = <<>>
    /\ applied = [n \in Nodes |-> 0]
    /\ acked = 0
    /\ reads = [r \in {} |-> 0]

(* The event being replayed, and the state its node ended up in. *)
e  == Trace[i]
nd == e.node
NewLog  == SubSeq(log[nd], 1, e.lk) \o e.la
NewTerm == e.term

(* The first rule from a list of <<name, holds>> pairs that does not hold. *)
RECURSIVE FirstBroken(_)
FirstBroken(rules) ==
    IF rules = <<>> THEN "ok"
    ELSE IF ~Head(rules)[2] THEN Head(rules)[1]
    ELSE FirstBroken(Tail(rules))

(* Raft's commit rules, checked after every step of a node:
   a node never commits past what a leader has committed, and its committed
   prefix is exactly the cluster's committed log. *)
CommitRules(c, l, cl) ==
    << <<"CommitAheadOfLeader",    c <= Len(cl)>>,
       <<"CommitPastOwnLog",       c <= Len(l)>>,
       <<"CommittedEntryDiverged", Prefix(l, c) = Prefix(cl, c)>> >>

(* Adopt the node's recorded state. *)
Adopt ==
    /\ term'   = [term EXCEPT ![nd] = e.term]
    /\ role'   = [role EXCEPT ![nd] = e.role]
    /\ vote'   = [vote EXCEPT ![nd] = e.vote]
    /\ log'    = [log EXCEPT ![nd] = NewLog]
    /\ commit' = [commit EXCEPT ![nd] = e.commit]

(* Every leader step refreshes what we know about the leader's log. *)
TrackLeaderLog ==
    llog' = IF e.role = "L" /\ (llog[nd][1] # e.term \/ Len(llog[nd][2]) <= Len(NewLog))
            THEN [llog EXCEPT ![nd] = <<e.term, NewLog>>]
            ELSE llog

Steady == /\ e.term >= term[nd] /\ e.commit >= commit[nd]

---------------------------------------------------------------------------

Restart ==
    /\ e.kind = "Restart"
    /\ Adopt
    /\ up' = [up EXCEPT ![nd] = TRUE]
    /\ applied' = [applied EXCEPT ![nd] = 0]
    /\ err' = FirstBroken(
         << <<"LostVoteOnRestart",
              \A g \in grants : g[1] = nd =>
                  (e.term > g[3] \/ (e.term = g[3] /\ e.vote = g[2]))>>,
            <<"LostAckedEntriesOnRestart", Prefix(NewLog, Len(dur[nd])) = dur[nd]>> >>
         \o CommitRules(e.commit, NewLog, clog))
    /\ UNCHANGED <<grants, leaders, llog, dur, clog, cterm, acked, reads>>

Crash ==
    /\ e.kind = "Crash"
    /\ up' = [up EXCEPT ![nd] = FALSE]
    /\ err' = "ok"
    /\ UNCHANGED <<term, role, vote, log, commit, grants, leaders, llog, dur,
                   clog, cterm, applied, acked, reads>>

RoleChange ==
    /\ e.kind \in {"BecomeFollower", "PreCandidate"}
    /\ Adopt
    /\ TrackLeaderLog
    /\ err' = FirstBroken(
         << <<"TermWentBackwards", Steady>>,
            <<"LogChangedOnRoleChange", NewLog = log[nd]>> >>
         \o CommitRules(e.commit, NewLog, clog))
    /\ UNCHANGED <<up, grants, leaders, dur, clog, cterm, applied, acked, reads>>

BecomeCandidate ==
    /\ e.kind = "BecomeCandidate"
    /\ Adopt
    /\ grants' = grants \cup {<<nd, nd, e.term>>}
    /\ err' = FirstBroken(
         << <<"TermWentBackwards", Steady>>,
            <<"CandidateMustVoteForItself", e.vote = nd>>,
            <<"DoubleVote", \A g \in grants : (g[1] = nd /\ g[3] = e.term) => g[2] = nd>> >>
         \o CommitRules(e.commit, NewLog, clog))
    /\ UNCHANGED <<up, leaders, llog, dur, clog, cterm, applied, acked, reads>>

(* A granted vote. The message carries the candidate's last log index and
   term; raft only grants if that log is at least as up to date. *)
Vote ==
    /\ e.kind = "Vote"
    /\ Adopt
    /\ grants' = grants \cup {<<nd, e.peer, e.mterm>>}
    /\ err' = FirstBroken(
         << <<"TermWentBackwards", Steady>>,
            <<"DoubleVote", \A g \in grants : (g[1] = nd /\ g[3] = e.mterm) => g[2] = e.peer>>,
            <<"VoteForStaleLog",
              \/ e.last_term > LastTerm(log[nd])
              \/ (e.last_term = LastTerm(log[nd]) /\ e.last_idx >= Len(log[nd]))>> >>
         \o CommitRules(e.commit, NewLog, clog))
    /\ UNCHANGED <<up, leaders, llog, dur, clog, cterm, applied, acked, reads>>

(* Leader completeness: a new leader holds every entry committed in an
   earlier term. *)
HasCommitted(l, t) ==
    \A k \in 1..Len(clog) : cterm[k] < t => (k <= Len(l) /\ l[k] = clog[k])

BecomeLeader ==
    /\ e.kind = "BecomeLeader"
    /\ Adopt
    /\ TrackLeaderLog
    /\ leaders' = leaders \cup {<<nd, e.term>>}
    /\ err' = FirstBroken(
         << <<"TermWentBackwards", Steady>>,
            <<"LeaderWithoutQuorum",
              Cardinality({v \in Nodes : <<v, nd, e.term>> \in grants}) >= Quorum>>,
            <<"TwoLeadersInOneTerm", \A l \in leaders : l[2] = e.term => l[1] = nd>>,
            <<"LeaderMissingCommittedEntry", HasCommitted(NewLog, e.term)>> >>
         \o CommitRules(e.commit, NewLog, clog))
    /\ UNCHANGED <<up, grants, dur, clog, cterm, applied, acked, reads>>

ClientRequest ==
    /\ e.kind = "ClientRequest"
    /\ Adopt
    /\ TrackLeaderLog
    /\ err' = FirstBroken(
         << <<"TermWentBackwards", Steady>>,
            <<"OnlyLeadersAppend", e.role = "L">>,
            <<"LeaderOnlyAppends", NewLog = Append(log[nd], e.term)>> >>
         \o CommitRules(e.commit, NewLog, clog))
    /\ UNCHANGED <<up, grants, leaders, dur, clog, cterm, applied, acked, reads>>

(* A follower accepted AppendEntries from leader e.peer for term e.mterm,
   covering indexes 1..prev+n. That prefix must equal the leader's log. *)
AppendOK ==
    LET k  == e.prev + e.n
        ll == llog[e.peer]
    IN
    /\ e.kind = "AppendOK"
    /\ Adopt
    /\ dur' = [dur EXCEPT ![nd] = Prefix(NewLog, k)]
    /\ err' = FirstBroken(
         << <<"TermWentBackwards", Steady>>,
            <<"AcceptedStaleLeader", e.mterm >= term[nd]>>,
            <<"AppendBeyondOwnLog", k <= Len(NewLog)>>,
            <<"LogMatching", ll[1] = e.mterm => (k <= Len(ll[2]) /\ Prefix(NewLog, k) = Prefix(ll[2], k))>> >>
         \o CommitRules(e.commit, NewLog, clog))
    /\ UNCHANGED <<up, grants, leaders, llog, clog, cterm, applied, acked, reads>>

(* The leader moved its commit index to e.commit. *)
LeaderCommit ==
    LET c  == e.commit
        old == Len(clog)
        cl == IF c > old THEN Prefix(NewLog, c) ELSE clog
        ct == IF c > old THEN cterm \o [k \in 1..(c - old) |-> e.term] ELSE cterm
        holders == IF c > Len(NewLog) THEN {}
                   ELSE {m \in Nodes \ {nd} : Len(log[m]) >= c /\ log[m][c] = NewLog[c]}
    IN
    /\ e.kind = "LeaderCommit"
    /\ Adopt
    /\ TrackLeaderLog
    /\ clog' = cl
    /\ cterm' = ct
    /\ err' = FirstBroken(
         << <<"TermWentBackwards", Steady>>,
            <<"OnlyLeadersCommit", e.role = "L">>,
            <<"CommitWithoutQuorum", Cardinality(holders) + 1 >= Quorum>>,
            <<"CommittedOldTermByCounting", c <= Len(NewLog) /\ NewLog[c] = e.term>>,
            <<"CommittedEntryChanged", Prefix(NewLog, Min(c, old)) = Prefix(clog, Min(c, old))>> >>
         \o CommitRules(c, NewLog, cl))
    /\ UNCHANGED <<up, grants, leaders, dur, applied, acked, reads>>

(* State machine safety: every node applies the committed entries, in order. *)
Apply ==
    /\ e.kind = "Apply"
    /\ Adopt
    /\ TrackLeaderLog
    /\ applied' = [applied EXCEPT ![nd] = e.hi]
    /\ err' = FirstBroken(
         << <<"TermWentBackwards", Steady>>,
            <<"AppliedOutOfOrder", e.lo = applied[nd] + 1>>,
            <<"AppliedUncommittedEntry", e.hi <= Len(clog)>>,
            <<"StateMachinesDiverged", e.hi <= Len(clog) => e.terms = SubSeq(clog, e.lo, e.hi)>> >>
         \o CommitRules(e.commit, NewLog, clog))
    /\ UNCHANGED <<up, grants, leaders, dur, clog, cterm, acked, reads>>

WriteAck ==
    /\ e.kind = "WriteAck"
    /\ acked' = Max(acked, e.idx)
    /\ err' = FirstBroken(
         << <<"AckedUncommittedWrite", e.idx <= Len(clog) /\ clog[e.idx] = e.mterm>> >>)
    /\ UNCHANGED <<term, role, vote, log, commit, up, grants, leaders, llog,
                   dur, clog, cterm, applied, reads>>

ReadStart ==
    /\ e.kind = "ReadStart"
    /\ Adopt
    /\ TrackLeaderLog
    /\ reads' = reads @@ (e.rid :> acked)
    /\ err' = FirstBroken(<< <<"TermWentBackwards", Steady>> >> \o CommitRules(e.commit, NewLog, clog))
    /\ UNCHANGED <<up, grants, leaders, dur, clog, cterm, applied, acked>>

(* Linearizable reads: a read must see every write acknowledged before it
   started. It reads the state machine as of index e.idx. *)
ReadServe ==
    /\ e.kind = "ReadServe"
    /\ Adopt
    /\ TrackLeaderLog
    /\ err' = FirstBroken(
         << <<"TermWentBackwards", Steady>>,
            <<"StaleRead", e.rid \in DOMAIN reads /\ e.idx >= reads[e.rid]>> >>
         \o CommitRules(e.commit, NewLog, clog))
    /\ UNCHANGED <<up, grants, leaders, dur, clog, cterm, applied, acked, reads>>

Step ==
    /\ i <= Len(Trace)
    /\ err = "ok"
    /\ \/ Restart \/ Crash \/ RoleChange \/ BecomeCandidate \/ Vote
       \/ BecomeLeader \/ ClientRequest \/ AppendOK \/ LeaderCommit \/ Apply
       \/ WriteAck \/ ReadStart \/ ReadServe
    /\ i' = i + 1
    \* report the first broken rule once, instead of TLC dumping every state
    /\ IF err' # "ok" THEN PrintT(<<"VIOLATION", i, err'>>) ELSE TRUE

(* Once the whole trace is replayed (or a rule broke), nothing more happens. *)
Done == (i > Len(Trace) \/ err # "ok") /\ UNCHANGED vars

Next == Step \/ Done

Spec == Init /\ [][Next]_vars

-------------------------------------------------------------------------------
(* The single safety condition: no rule was broken. The checker reads the
   VIOLATION line printed by Step rather than asking TLC for a counterexample,
   because a counterexample would print every replayed state. *)
Ok == err = "ok"

(* The whole trace was replayed. *)
Replayed == i = Len(Trace) + 1
=============================================================================
