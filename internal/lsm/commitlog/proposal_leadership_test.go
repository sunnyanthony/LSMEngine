package commitlog

import (
	"context"
	"testing"
	"time"
)

func TestMultiPeerProposalDoesNotDriveElection(t *testing.T) {
	for _, kind := range []string{"data", "control"} {
		t.Run(kind, func(t *testing.T) {
			transport := &recordingRaftTransport{}
			c, err := newEtcdRaftConsensus(Config{
				Provider: ProviderEtcdRaft, DataDir: t.TempDir(),
				NodeID: "node-a", Peers: []string{"node-a", "node-b", "node-c"},
				Transport: transport,
			})
			if err != nil {
				t.Fatal(err)
			}
			before := c.rawNode.Status()
			messageCount := len(transport.messagesCopy())
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if kind == "data" {
				_, err = c.CommitData(ctx, DataMutation{Kind: "put", Key: []byte("key"), Value: []byte("value")})
			} else {
				_, err = c.CommitControl(ctx, ControlMutation{Kind: "transfer-leader", ShardID: "shared", Target: "node-b"})
			}
			if err == nil {
				t.Fatal("proposal without an elected leader unexpectedly succeeded")
			}
			after := c.rawNode.Status()
			if after.Term != before.Term || after.RaftState != before.RaftState {
				t.Errorf("proposal drove election: before term=%d state=%v; after term=%d state=%v", before.Term, before.RaftState, after.Term, after.RaftState)
			}
			if got := len(transport.messagesCopy()) - messageCount; got != 0 {
				t.Errorf("proposal emitted %d peer messages without an elected leader", got)
			}
			if len(c.pending) != 0 || len(c.CommittedEntriesAfter(0)) != 0 {
				t.Fatal("rejected proposal retained pending or committed mutations")
			}
		})
	}
}
