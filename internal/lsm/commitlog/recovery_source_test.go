package commitlog

import "testing"

func TestCommittedEntriesCursorDoesNotConsumeHistory(t *testing.T) {
	c := &etcdRaftConsensus{committed: []raftCommittedProposal{
		{Data: &DataCommittedEntry{Commit: Commit{Index: 3, Term: 1}, Seq: 3}},
		{Control: &ControlCommittedEntry{Commit: Commit{Index: 5, Term: 1}, Mutation: ControlMutation{Kind: "split", Split: []byte("m")}}},
		{Data: &DataCommittedEntry{Commit: Commit{Index: 7, Term: 1}, Seq: 7}},
	}}
	for _, cursor := range []uint64{0, 3, 4, 5, 7, 8} {
		for attempt := 0; attempt < 2; attempt++ {
			entries := c.CommittedEntriesAfter(cursor)
			var want int
			for _, index := range []uint64{3, 5, 7} {
				if index > cursor {
					want++
				}
			}
			if len(entries) != want {
				t.Fatalf("cursor %d: got %d, want %d", cursor, len(entries), want)
			}
			previous := cursor
			for _, entry := range entries {
				var index uint64
				if entry.Data != nil {
					index = entry.Data.Commit.Index
				} else {
					index = entry.Control.Commit.Index
					entry.Control.Mutation.Split[0] = 'X'
				}
				if index <= previous {
					t.Fatalf("non-increasing cursor %d after %d", index, previous)
				}
				previous = index
			}
		}
	}
	if got := c.RecoveredEntries(); len(got) != 3 || string(got[1].Control.Mutation.Split) != "m" {
		t.Fatal("cursor consumed or aliased retained history")
	}
}

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
