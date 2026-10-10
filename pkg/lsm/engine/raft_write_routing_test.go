package engine

import (
	"errors"
	"testing"

	"lsmengine/pkg/lsm/errs"
)

func TestRaftWriteRoutingOverridesMetadataWithoutMutatingIt(t *testing.T) {
	stores, _, _ := newApplyPairBeforeElection(t, func(store *LSM) {
		shard := store.Shards()[0]
		if shard.WriteLeader == nil || *shard.WriteLeader != "" {
			t.Fatal("unelected raft node advertised metadata leader as write authority")
		}
	})
	if err := stores[0].TransferLeader("shared", "node-b"); err != nil {
		t.Fatal(err)
	}
	if err := stores[0].Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := stores[1].Delete([]byte("key")); !errors.Is(err, errs.ErrNotLeader) {
		t.Fatalf("follower admitted write: %v", err)
	}
	for _, store := range stores {
		shard := store.Shards()[0]
		if shard.Leader != "node-b" || shard.WriteLeader == nil || *shard.WriteLeader != "node-a" {
			t.Fatalf("metadata/runtime leaders confused: %+v", shard)
		}
		*shard.WriteLeader = "tampered"
		if got := store.Shards()[0]; *got.WriteLeader != "node-a" {
			t.Fatal("snapshot did not own write leader")
		}
		if store.ClusterStatus().Revision != 1 {
			t.Fatal("leadership observation mutated revision")
		}
	}
	if err := stores[0].Delete([]byte("key")); err != nil {
		t.Fatal(err)
	}
}
