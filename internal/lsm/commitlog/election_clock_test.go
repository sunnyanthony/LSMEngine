package commitlog

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

type queuedElectionTransport struct {
	messages chan []PeerMessage
}

func (t *queuedElectionTransport) Send(_ context.Context, messages []PeerMessage) error {
	t.messages <- messages
	return nil
}

func TestCommitPollingDoesNotAccelerateElectionClock(t *testing.T) {
	transport := &queuedElectionTransport{messages: make(chan []PeerMessage, 2048)}
	consensus, err := newEtcdRaftConsensus(Config{Provider: ProviderEtcdRaft, DataDir: t.TempDir(), NodeID: "node-a",
		Peers: []string{"node-a", "node-b"}, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	cleanupEtcdRaftConsensus(t, consensus)
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := consensus.CommitData(ctx, DataMutation{Kind: "put", Key: []byte("key"), Value: []byte("value")})
		done <- err
	}()
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("30ms peer response latency must not cause an election storm: %v", err)
			}
			return
		case messages := <-transport.messages:
			// Longer than ten 1ms request polls, but comfortably shorter than
			// ten real background ticks (the configured election timeout).
			time.Sleep(30 * time.Millisecond)
			decoded, err := decodeRaftPeerMessages(messages)
			if err != nil {
				cancel()
				<-done
				t.Fatal(err)
			}
			var responses []raftpb.Message
			for _, message := range decoded {
				response := raftpb.Message{From: message.To, To: message.From, Term: message.Term}
				switch message.Type {
				case raftpb.MsgPreVote:
					response.Type = raftpb.MsgPreVoteResp
				case raftpb.MsgVote:
					response.Type = raftpb.MsgVoteResp
				case raftpb.MsgApp:
					response.Type, response.Index = raftpb.MsgAppResp, message.Index
					if len(message.Entries) > 0 {
						response.Index = message.Entries[len(message.Entries)-1].Index
					}
				case raftpb.MsgHeartbeat:
					response.Type = raftpb.MsgHeartbeatResp
				default:
					continue
				}
				responses = append(responses, response)
			}
			encoded, err := encodeRaftPeerMessages(responses)
			if err == nil {
				err = consensus.HandlePeerMessages(context.Background(), encoded)
			}
			if err != nil {
				cancel()
				<-done
				t.Fatal(err)
			}
		}
	}
}
