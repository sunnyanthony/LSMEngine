package commitlog

import (
	"context"
	"errors"
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
			lastIndex, _ := c.storage.LastIndex()
			messageCount := len(transport.messagesCopy())
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if kind == "data" {
				_, err = c.CommitData(ctx, DataMutation{Kind: "put", Key: []byte("key"), Value: []byte("value")})
			} else {
				_, err = c.CommitControl(ctx, ControlMutation{Kind: "transfer-leader", ShardID: "shared", Target: "node-b"})
			}
			if !errors.Is(err, ErrNotLeader) {
				t.Fatalf("expected pre-proposal leadership rejection, got %v", err)
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
			if index, _ := c.storage.LastIndex(); index != lastIndex || c.proposalSeq != 0 {
				t.Fatal("rejected proposal changed log or proposal counter")
			}
		})
	}
}

func TestPendingProposalAllowsIndependentProgress(t *testing.T) {
	for _, kind := range []string{"data", "control"} {
		for _, cancelBeforeDelivery := range []bool{false, true} {
			name := kind + "/commit"
			if cancelBeforeDelivery {
				name = kind + "/cancel"
			}
			t.Run(name, func(t *testing.T) {
				transport := &recordingRaftTransport{}
				nodes := make(map[uint64]*etcdRaftConsensus)
				var leader *etcdRaftConsensus
				for _, name := range []string{"node-a", "node-b"} {
					c, err := newEtcdRaftConsensus(Config{DataDir: t.TempDir(), NodeID: name, Peers: []string{"node-a", "node-b"}, Transport: transport})
					if err != nil {
						t.Fatal(err)
					}
					nodes[c.nodeID] = c
					if leader == nil {
						leader = c
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				pump := func() {
					t.Helper()
					for round := 0; round < 100; round++ {
						transport.mu.Lock()
						messages := transport.messages
						transport.messages = nil
						transport.mu.Unlock()
						if len(messages) == 0 {
							return
						}
						for _, message := range messages {
							if err := nodes[message.To].HandlePeerMessages(ctx, []PeerMessage{message}); err != nil {
								t.Fatal(err)
							}
						}
					}
					t.Fatal("message delivery did not settle")
				}
				for i := 0; i < 30 && !leader.RuntimeStatus().Leader; i++ {
					if err := leader.Tick(ctx); err != nil {
						t.Fatal(err)
					}
					pump()
				}
				if !leader.RuntimeStatus().Leader {
					t.Fatal("independent election failed")
				}
				proposalCtx, stop := context.WithCancel(ctx)
				defer stop()
				result := make(chan error, 1)
				go func() {
					var err error
					if kind == "data" {
						_, err = leader.CommitData(proposalCtx, DataMutation{Kind: "put", Key: []byte("key"), Value: []byte("value")})
					} else {
						_, err = leader.CommitControl(proposalCtx, ControlMutation{Kind: "transfer-leader", ShardID: "shared", Target: "node-b"})
					}
					result <- err
				}()
				for len(transport.messagesCopy()) == 0 {
					select {
					case err := <-result:
						t.Fatalf("proposal ended before delivery: %v", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					default:
					}
					time.Sleep(time.Millisecond)
				}
				// No replies are delivered yet. Tick must acquire the provider mutex
				// independently while the proposer is waiting, not after its timeout.
				ticked := make(chan error, 1)
				go func() { ticked <- leader.Tick(ctx) }()
				select {
				case err := <-ticked:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("pending proposal blocked Tick")
				}
				if cancelBeforeDelivery {
					stop()
					if err := <-result; !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation: %v", err)
					}
					leader.mu.Lock()
					pending := len(leader.pending)
					leader.mu.Unlock()
					if pending != 0 {
						t.Fatal("cancellation leaked pending waiter")
					}
				}
				pump()
				if !cancelBeforeDelivery {
					select {
					case err := <-result:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal("asynchronous replies did not complete proposal")
					}
				}
				if len(leader.CommittedEntriesAfter(0)) != 1 {
					t.Fatal("committed entry lost after delayed delivery")
				}
			})
		}
	}
}
