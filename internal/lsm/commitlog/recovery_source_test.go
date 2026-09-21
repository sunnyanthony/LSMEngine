package commitlog

import "testing"

func TestRecoveredEntriesPreserveOrderAndOwnership(t *testing.T) {
	c := &etcdRaftConsensus{committed: []raftCommittedProposal{
		{Data: &DataCommittedEntry{Commit: Commit{Index: 3, Term: 1}, Seq: 3, Mutation: DataMutation{Kind: "put", Key: []byte("key"), Value: []byte("value")}}},
		{Control: &ControlCommittedEntry{Commit: Commit{Index: 4, Term: 1}, Mutation: ControlMutation{Kind: "split", Split: []byte("middle")}}},
	}}
	first := c.RecoveredEntries()
	if len(first) != 2 || first[0].Data.Commit.Index != 3 || first[1].Control.Commit.Index != 4 {
		t.Fatalf("recovery reordered entries: %+v", first)
	}
	first[0].Data.Mutation.Key[0] = 'X'
	first[0].Data.Mutation.Value[0] = 'X'
	first[0].Data.Seq = 99
	first[1].Control.Mutation.Split[0] = 'X'
	first[1].Control.Commit.Index = 99
	second := c.RecoveredEntries()
	if string(second[0].Data.Mutation.Key) != "key" || string(second[0].Data.Mutation.Value) != "value" || second[0].Data.Seq != 3 || string(second[1].Control.Mutation.Split) != "middle" || second[1].Control.Commit.Index != 4 {
		t.Fatalf("caller mutated recovery history: %+v", second)
	}
}
