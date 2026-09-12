package commitlog

import (
	"context"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestJoinSnapshotRequiresRecipientMembership(t *testing.T) {
	for _, includeJoiner := range []bool{false, true} {
		name := "before-membership-change"
		if includeJoiner {
			name = "after-membership-change"
		}
		t.Run(name, func(t *testing.T) {
			consensus, err := newEtcdRaftConsensus(Config{
				Provider: ProviderEtcdRaft, DataDir: t.TempDir(),
				NodeID: "node-d", Peers: []string{"node-a", "node-b", "node-c", "node-d"},
				Transport: &recordingRaftTransport{}, Join: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			cleanupEtcdRaftConsensus(t, consensus)
			applier := &recordingStateSnapshotApplier{}
			if err := consensus.SetStateSnapshotApplier(applier); err != nil {
				t.Fatal(err)
			}
			voters := []uint64{stableRaftNodeID("node-a"), stableRaftNodeID("node-b"), stableRaftNodeID("node-c")}
			if includeJoiner {
				voters = append(voters, consensus.nodeID)
			}
			messages, err := encodeRaftPeerMessages([]raftpb.Message{{
				Type: raftpb.MsgSnap, From: voters[0], To: consensus.nodeID, Term: 3,
				Snapshot: raftpb.Snapshot{
					Metadata: raftpb.SnapshotMetadata{Index: 8, Term: 2, ConfState: raftpb.ConfState{Voters: voters}},
					Data:     []byte("state-8"),
				},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err := consensus.HandlePeerMessages(context.Background(), messages); err != nil {
				t.Fatal(err)
			}
			status := consensus.RuntimeStatus()
			if !status.LeaderKnown {
				t.Fatal("snapshot sender should be recognized as leader even when snapshot is rejected")
			}
			if !includeJoiner {
				if status.Index != 0 || status.SnapshotIndex != 0 || len(applier.indexes) != 0 {
					t.Fatalf("snapshot excluding recipient must not apply: status=%+v applied=%v", status, applier.indexes)
				}
				// A later snapshot at the membership boundary can unblock the
				// same joiner; do not rewrite membership at the old log index.
				messages, err = encodeRaftPeerMessages([]raftpb.Message{{
					Type: raftpb.MsgSnap, From: voters[0], To: consensus.nodeID, Term: 3,
					Snapshot: raftpb.Snapshot{
						Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 3, ConfState: raftpb.ConfState{Voters: append(voters, consensus.nodeID)}},
						Data:     []byte("state-9"),
					},
				}})
				if err != nil {
					t.Fatal(err)
				}
				if err := consensus.HandlePeerMessages(context.Background(), messages); err != nil {
					t.Fatal(err)
				}
				if status := consensus.RuntimeStatus(); status.Index != 9 || len(applier.indexes) != 1 || applier.indexes[0] != 9 {
					t.Fatalf("later membership snapshot must unblock joiner: status=%+v applied=%v", status, applier.indexes)
				}
				return
			}
			if status.Index != 8 || status.SnapshotIndex != 8 || len(applier.indexes) != 1 || applier.indexes[0] != 8 {
				t.Fatalf("snapshot including recipient should apply: status=%+v applied=%v", status, applier.indexes)
			}
		})
	}
}

// This characterizes the replacement blocker: membership-only progress does
// not produce a fresh engine snapshot, leaving a compacted joiner unable to use
// the snapshot that the leader has available. Replace this expectation with a
// catch-up assertion when ordered membership snapshot boundaries are supported.
func TestMembershipChangeLeavesPreJoinSnapshotAvailable(t *testing.T) {
	consensus, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft, DataDir: t.TempDir(), NodeID: "node-a",
		Transport: &recordingRaftTransport{}, SnapshotPolicy: SnapshotPolicy{AppliedEntries: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupEtcdRaftConsensus(t, consensus)
	if err := consensus.SetStateSnapshotter(&recordingStateSnapshotter{}); err != nil {
		t.Fatal(err)
	}
	entry, err := consensus.CommitData(context.Background(), DataMutation{Kind: "put", Key: []byte("key"), Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	consensus.ObserveCommittedIndex(entry.Commit.Index)
	if err := consensus.ChangeMembership(context.Background(), MembershipChange{Type: MembershipChangeAddNode, NodeID: "node-d"}); err != nil {
		t.Fatal(err)
	}
	consensus.mu.Lock()
	defer consensus.mu.Unlock()
	snapshot, err := consensus.storage.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	first, err := consensus.storage.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Metadata.Index != entry.Commit.Index || first != entry.Commit.Index+1 || consensus.index <= snapshot.Metadata.Index {
		t.Fatalf("expected compacted pre-membership snapshot: snapshot=%+v first=%d committed=%d", snapshot.Metadata, first, consensus.index)
	}
	joiner := stableRaftNodeID("node-d")
	if !consensus.membershipChangeAppliedLocked(raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: joiner}) {
		t.Fatal("joiner membership was not applied")
	}
	for _, voter := range snapshot.Metadata.ConfState.Voters {
		if voter == joiner {
			t.Fatal("pre-membership snapshot unexpectedly includes future joiner")
		}
	}
}
