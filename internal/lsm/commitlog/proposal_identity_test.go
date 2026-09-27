package commitlog

import (
	"encoding/json"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestPendingProposalRejectsForeignAndStaleIdentity(t *testing.T) {
	local := raftCommitProposal{ID: 1, Proposer: 2, RequestID: "current-incarnation", Kind: "data", Data: &DataMutation{Kind: "put", Key: []byte("local"), Value: []byte("value")}}
	payload, err := json.Marshal(local)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"foreign-node", "old-incarnation", "wrong-mutation", "legacy"} {
		t.Run(variant, func(t *testing.T) {
			pending := &pendingRaftProposal{payload: payload}
			c := &etcdRaftConsensus{pending: map[uint64]*pendingRaftProposal{1: pending}}
			other := local
			switch variant {
			case "foreign-node":
				other.Proposer = 3
			case "old-incarnation":
				other.RequestID = "previous-incarnation"
			case "wrong-mutation":
				other.Data = &DataMutation{Kind: "put", Key: []byte("foreign"), Value: []byte("value")}
			case "legacy":
				other.Proposer = 0
				other.RequestID = ""
			}
			foreign, err := json.Marshal(other)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.applyCommittedEntryLocked(raftpb.Entry{Type: raftpb.EntryNormal, Term: 2, Index: 4, Data: foreign}); err != nil {
				t.Fatal(err)
			}
			if pending.done || c.pending[1] != pending {
				t.Fatal("foreign committed entry completed local waiter")
			}
			if len(c.committed) != 1 {
				t.Fatal("foreign entry was dropped from apply stream")
			}
			if err := c.applyCommittedEntryLocked(raftpb.Entry{Type: raftpb.EntryNormal, Term: 2, Index: 5, Data: payload}); err != nil {
				t.Fatal(err)
			}
			if !pending.done || pending.data == nil || pending.data.Commit.Index != 5 || string(pending.data.Mutation.Key) != "local" {
				t.Fatal("local proposal did not match its own entry")
			}
		})
	}
}
