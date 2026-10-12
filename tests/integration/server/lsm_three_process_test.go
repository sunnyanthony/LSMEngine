//go:build test && !windows

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/server"
	serverconfig "lsmengine/pkg/lsm/server/config"
)

// This exercises the shipped executable, not an in-process HTTP handler.
func TestThreeProcessClusterRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and starts three server processes")
	}
	root := t.TempDir()
	binary := filepath.Join(root, "lsmctl")
	run := func(args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v\n%s", args, err, out)
		}
		return out
	}
	// Docker mounts may not expose usable Git metadata; version stamping is not
	// part of the executable behavior under test.
	run("go", "build", "-buildvcs=false", "-o", binary, "../../../cmd/lsmctl")
	names := []string{"node-a", "node-b", "node-c"}
	endpoints := map[string]string{}
	listeners := make([]net.Listener, 3)
	for i, name := range names {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = listener
		t.Cleanup(func() { _ = listener.Close() })
		endpoints[name] = "http://" + listener.Addr().String()
	}
	type process struct {
		cmd  *exec.Cmd
		done chan error
		log  *os.File
	}
	processes := make([]*process, 3)
	configs := make([]serverconfig.Config, 3)
	paths := make([]string, 3)
	stop := func(i int) error {
		p := processes[i]
		if p == nil {
			return nil
		}
		processes[i] = nil
		_ = p.cmd.Process.Signal(os.Interrupt)
		var err error
		select {
		case err = <-p.done:
		case <-time.After(15 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
			err = fmt.Errorf("%s did not shut down", names[i])
		}
		_ = p.log.Close()
		return err
	}
	t.Cleanup(func() {
		for i := range processes {
			if err := stop(i); err != nil {
				t.Errorf("cleanup: %v", err)
			}
			if t.Failed() {
				data, _ := os.ReadFile(filepath.Join(root, names[i]+".log"))
				t.Logf("%s:\n%s", names[i], data)
			}
		}
	})
	for i, name := range names {
		configs[i] = serverconfig.Config{
			DataDir: filepath.Join(root, name), NodeID: name, ClusterID: "process-smoke",
			Addr:      listeners[i].Addr().String(),
			CommitLog: serverconfig.CommitLogConfig{Provider: "etcd-raft"},
			Raft:      serverconfig.RaftConfig{Peers: names, PeerURLs: endpoints},
			Shards:    []serverconfig.ShardConfig{{ID: "shared", Leader: names[0], Replicas: names}},
		}
		data, err := yaml.Marshal(configs[i])
		if err != nil {
			t.Fatal(err)
		}
		paths[i] = filepath.Join(root, name+".yaml")
		if err := os.WriteFile(paths[i], data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	start := func(i int) {
		t.Helper()
		log, err := os.OpenFile(filepath.Join(root, names[i]+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(binary, "serve", "--config", paths[i])
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			_ = log.Close()
			t.Fatal(err)
		}
		p := &process{cmd: cmd, done: make(chan error, 1), log: log}
		processes[i] = p
		go func() { p.done <- cmd.Wait() }()
	}
	for i, listener := range listeners {
		// The executable owns its listener; a port collision fails readiness below.
		_ = listener.Close()
		start(i)
	}
	client := &http.Client{Timeout: 500 * time.Millisecond}
	defer client.CloseIdleConnections()
	read := func(i int, path string, out any) bool {
		resp, err := client.Get(endpoints[names[i]] + path)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(out) == nil
	}
	await := func(label string, ready func() bool) {
		t.Helper()
		deadline := time.Now().Add(25 * time.Second)
		for time.Now().Before(deadline) {
			if ready() {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		for i, p := range processes {
			if p == nil {
				continue
			}
			var status lsm.ClusterStatus
			started := time.Now()
			ok := read(i, "/cluster/status", &status)
			t.Logf("%s status available=%v latency=%s runtime=%+v", names[i], ok, time.Since(started), status.CommitLogRuntime)
		}
		t.Fatalf("timed out: %s", label)
	}
	leader := -1
	awaitLeader := func() {
		await("running nodes agree on leader", func() bool {
			candidate := ""
			leader = -1
			for i, p := range processes {
				if p == nil {
					continue
				}
				var status lsm.ClusterStatus
				if !read(i, "/cluster/status", &status) || status.NodeID != names[i] || status.CommitLogRuntime.LeaderNodeID == "" {
					return false
				}
				if candidate != "" && candidate != status.CommitLogRuntime.LeaderNodeID {
					return false
				}
				candidate = status.CommitLogRuntime.LeaderNodeID
				if status.CommitLogRuntime.Leader {
					leader = i
				}
			}
			return leader >= 0 && names[leader] == candidate
		})
	}
	endpointJSON, err := json.Marshal(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	write := func(operation, key, value string) {
		t.Helper()
		args := []string{binary, operation, "--bootstrap-url", endpoints[names[0]], "--node-endpoints", string(endpointJSON), "--key", key, "--timeout", "15s"}
		if operation == "cluster-put" {
			args = append(args, "--value", value)
		}
		var result lsm.WriteRequestStatus
		if err := json.Unmarshal(run(args...), &result); err != nil || string(result.State) != "committed" {
			t.Fatalf("CLI did not acknowledge commitment: %+v, %v", result, err)
		}
	}
	awaitReplicas := func() {
		var target lsm.Stats
		if !read(leader, "/stats", &target) || target.Seq == 0 {
			t.Fatal("leader sequence unavailable")
		}
		await("replica sequence catch-up", func() bool {
			for i := range processes {
				var stats lsm.Stats
				if !read(i, "/stats", &stats) || stats.Seq < target.Seq {
					return false
				}
			}
			return true
		})
	}
	stopAll := func() {
		for i := range processes {
			if err := stop(i); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Log("initial election and writes")
	awaitLeader()
	write("cluster-put", "retained", "before-restart")
	write("cluster-put", "deleted", "remove-me")
	awaitReplicas()
	stopped := leader
	t.Logf("stop leader %s; elect replacement and write", names[stopped])
	if err := stop(stopped); err != nil {
		t.Fatal(err)
	}
	awaitLeader()
	write("cluster-put", "offline", "while-leader-stopped")
	write("cluster-delete", "deleted", "")
	t.Logf("restart %s and wait for catch-up", names[stopped])
	start(stopped)
	awaitLeader()
	awaitReplicas()
	t.Log("stop and restart all nodes")
	stopAll()
	for i := range processes {
		start(i)
	}
	awaitLeader()
	write("cluster-put", "restarted", "after-full-restart")
	awaitReplicas()
	stopAll()
	t.Log("verify each stopped replica through engine recovery")
	// Open only after all processes exit. Verify actual values, not just counters.
	for i, cfg := range configs {
		peerURLs := map[uint64]string{}
		for name, url := range endpoints {
			peerURLs[lsm.RaftPeerID(name)] = url
		}
		transport, err := server.NewRaftHTTPTransport(server.RaftHTTPTransportOptions{PeerURLs: peerURLs})
		if err != nil {
			t.Fatal(err)
		}
		store, err := lsm.New(lsm.Options{DataDir: cfg.DataDir, NodeID: names[i], ClusterID: cfg.ClusterID,
			Raft:      &lsm.RaftOptions{Peers: names},
			CommitLog: &lsm.CommitLogOptions{Provider: lsm.CommitLogProviderEtcdRaft, Transport: transport},
			ShardMap:  []lsm.ShardConfig{{ID: "shared", Leader: names[0], Replicas: names}},
		})
		if err != nil {
			t.Fatal(err)
		}
		for key, want := range map[string]string{"retained": "before-restart", "offline": "while-leader-stopped", "restarted": "after-full-restart"} {
			got, ok := store.Get([]byte(key))
			if !ok || string(got.Value) != want {
				t.Errorf("%s key %q: got %+v, found=%v; want %q", names[i], key, got, ok, want)
			}
		}
		if _, ok := store.Get([]byte("deleted")); ok {
			t.Errorf("%s resurrected deleted key", names[i])
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
