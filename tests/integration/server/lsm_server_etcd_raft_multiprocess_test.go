//go:build test

package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestEtcdRaftThreeProcessSmoke(t *testing.T) {
	tempDir := t.TempDir()
	bin := buildLSMCTLBinary(t, tempDir)

	peers := []string{"node-a", "node-b", "node-c"}
	urls := reserveNodeURLs(t, peers)
	var processes []*startedLSMProcess
	for _, nodeID := range peers {
		configPath := writeProcessConfig(t, tempDir, processConfigOptions{
			NodeID:        nodeID,
			RaftPeers:     peers,
			ShardReplicas: peers,
			PeerURLs:      urlsForPeers(peers, urls),
		})
		proc := startLSMProcess(t, bin, configPath)
		processes = append(processes, proc)
	}
	t.Cleanup(func() {
		for i := len(processes) - 1; i >= 0; i-- {
			processes[i].stop(t)
		}
	})

	for _, nodeID := range peers {
		waitHTTPStatus(t, urls[nodeID]+"/healthz", http.StatusOK, 5*time.Second)
	}

	key := []byte("k")
	value := []byte("multi-process")
	eventually(t, 5*time.Second, func() bool {
		status, body, err := postKVPut(urls["node-a"], key, value)
		if err != nil {
			return false
		}
		if status != http.StatusOK {
			t.Logf("leader put status=%d body=%s", status, body)
			return false
		}
		return true
	})

	for _, nodeID := range peers {
		nodeID := nodeID
		t.Run(nodeID, func(t *testing.T) {
			eventually(t, 5*time.Second, func() bool {
				got, ok, err := getKVValue(urls[nodeID], key)
				return err == nil && ok && bytes.Equal(got, value)
			})
		})
	}

	runLSMCTL(t, bin, "put", "--addr", urls["node-a"], "--key", "cli", "--value", "value")
	eventually(t, 5*time.Second, func() bool {
		out := runLSMCTL(t, bin, "get", "--addr", urls["node-b"], "--key", "cli")
		return bytes.Contains(out, []byte("found=true")) && bytes.Contains(out, []byte("value=value"))
	})
	runLSMCTL(t, bin, "delete", "--addr", urls["node-a"], "--key", "cli")
	eventually(t, 5*time.Second, func() bool {
		out := runLSMCTL(t, bin, "get", "--addr", urls["node-c"], "--key", "cli")
		return bytes.Contains(out, []byte("found=false"))
	})
}

func TestEtcdRaftThreeProcessLeaderRestartSmoke(t *testing.T) {
	tempDir := t.TempDir()
	bin := buildLSMCTLBinary(t, tempDir)

	peers := []string{"node-a", "node-b", "node-c"}
	urls := reserveNodeURLs(t, peers)
	processes := make(map[string]*startedLSMProcess, len(peers))
	configPaths := make(map[string]string, len(peers))
	for _, nodeID := range peers {
		configPath := writeProcessConfig(t, tempDir, processConfigOptions{
			NodeID:        nodeID,
			RaftPeers:     peers,
			ShardReplicas: peers,
			PeerURLs:      urlsForPeers(peers, urls),
		})
		configPaths[nodeID] = configPath
		processes[nodeID] = startLSMProcess(t, bin, configPath)
	}
	t.Cleanup(func() {
		for _, nodeID := range []string{"node-c", "node-b", "node-a"} {
			if proc := processes[nodeID]; proc != nil {
				proc.stop(t)
			}
		}
	})

	for _, nodeID := range peers {
		waitHTTPStatus(t, urls[nodeID]+"/healthz", http.StatusOK, 5*time.Second)
	}

	eventually(t, 10*time.Second, func() bool {
		status, body, err := postKVPut(urls["node-a"], []byte("before-leader-restart"), []byte("stable"))
		if err != nil {
			return false
		}
		if status != http.StatusOK {
			t.Logf("node-a initial put status=%d body=%s", status, body)
			return false
		}
		return true
	})
	for _, nodeID := range peers {
		nodeID := nodeID
		t.Run("before-restart-"+nodeID, func(t *testing.T) {
			eventually(t, 5*time.Second, func() bool {
				got, ok, err := getKVValue(urls[nodeID], []byte("before-leader-restart"))
				return err == nil && ok && bytes.Equal(got, []byte("stable"))
			})
		})
	}

	status, body, err := postShardTransferLeader(urls["node-a"], "users", "node-b")
	if err != nil {
		t.Fatalf("transfer shard leader to node-b: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("expected transfer-leader status 200, got %d (%s)", status, body)
	}
	eventually(t, 5*time.Second, func() bool {
		leader, err := getRouteLeader(urls["node-b"], "users")
		return err == nil && leader == "node-b"
	})

	processes["node-a"].stop(t)
	processes["node-a"] = nil
	eventually(t, 10*time.Second, func() bool {
		status, body, err := postKVPut(urls["node-b"], []byte("while-node-a-down"), []byte("quorum-survived"))
		if err != nil {
			return false
		}
		if status != http.StatusOK {
			t.Logf("node-b during-stop put status=%d body=%s", status, body)
			return false
		}
		return true
	})
	eventually(t, 10*time.Second, func() bool {
		got, ok, err := getKVValue(urls["node-c"], []byte("while-node-a-down"))
		return err == nil && ok && bytes.Equal(got, []byte("quorum-survived"))
	})

	processes["node-a"] = startLSMProcess(t, bin, configPaths["node-a"])
	waitHTTPStatus(t, urls["node-a"]+"/healthz", http.StatusOK, 5*time.Second)
	eventually(t, 10*time.Second, func() bool {
		got, ok, err := getKVValue(urls["node-a"], []byte("while-node-a-down"))
		return err == nil && ok && bytes.Equal(got, []byte("quorum-survived"))
	})
}

func TestEtcdRaftThreeProcessDynamicJoinSmoke(t *testing.T) {
	tempDir := t.TempDir()
	bin := buildLSMCTLBinary(t, tempDir)

	initialPeers := []string{"node-a", "node-b"}
	joinPeers := []string{"node-a", "node-b", "node-c"}
	urls := reserveNodeURLs(t, joinPeers)
	var processes []*startedLSMProcess
	configPaths := make(map[string]string, len(joinPeers))
	start := func(opts processConfigOptions) *startedLSMProcess {
		t.Helper()
		configPath := writeProcessConfig(t, tempDir, opts)
		configPaths[opts.NodeID] = configPath
		proc := startLSMProcess(t, bin, configPath)
		processes = append(processes, proc)
		return proc
	}
	for _, nodeID := range initialPeers {
		start(processConfigOptions{
			NodeID:        nodeID,
			RaftPeers:     initialPeers,
			ShardReplicas: initialPeers,
			PeerURLs:      urlsForPeers(initialPeers, urls),
			JoinPeerURLs:  urlsForPeers([]string{"node-c"}, urls),
		})
	}
	t.Cleanup(func() {
		for i := len(processes) - 1; i >= 0; i-- {
			processes[i].stop(t)
		}
	})

	for _, nodeID := range initialPeers {
		waitHTTPStatus(t, urls[nodeID]+"/healthz", http.StatusOK, 5*time.Second)
	}

	seedKey := []byte("seed")
	seedValue := []byte("before-join")
	eventually(t, 5*time.Second, func() bool {
		status, body, err := postKVPut(urls["node-a"], seedKey, seedValue)
		if err != nil {
			return false
		}
		if status != http.StatusOK {
			t.Logf("seed put status=%d body=%s", status, body)
			return false
		}
		return true
	})
	eventually(t, 5*time.Second, func() bool {
		got, ok, err := getKVValue(urls["node-b"], seedKey)
		return err == nil && ok && bytes.Equal(got, seedValue)
	})

	start(processConfigOptions{
		NodeID:        "node-c",
		RaftPeers:     joinPeers,
		ShardReplicas: joinPeers,
		PeerURLs:      urlsForPeers(joinPeers, urls),
		Join:          true,
	})
	waitHTTPStatus(t, urls["node-c"]+"/healthz", http.StatusOK, 5*time.Second)
	status, body, err := postRaftAdd(urls["node-a"], "node-c")
	if err != nil {
		t.Fatalf("raft-add node-c: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("expected raft-add status 200, got %d (%s)", status, body)
	}

	runLSMCTL(t, bin, "put", "--addr", urls["node-a"], "--key", "joined", "--value", "after-join")
	eventually(t, 10*time.Second, func() bool {
		out := runLSMCTL(t, bin, "get", "--addr", urls["node-c"], "--key", "joined")
		return bytes.Contains(out, []byte("found=true")) && bytes.Contains(out, []byte("value=after-join"))
	})

	processes[len(processes)-1].stop(t)
	processes = processes[:len(processes)-1]
	processes = append(processes, startLSMProcess(t, bin, configPaths["node-c"]))
	waitHTTPStatus(t, urls["node-c"]+"/healthz", http.StatusOK, 5*time.Second)
	eventually(t, 10*time.Second, func() bool {
		out := runLSMCTL(t, bin, "get", "--addr", urls["node-c"], "--key", "joined")
		return bytes.Contains(out, []byte("found=true")) && bytes.Contains(out, []byte("value=after-join"))
	})

	runLSMCTL(t, bin, "put", "--addr", urls["node-a"], "--key", "after-restart", "--value", "still-replicates")
	eventually(t, 10*time.Second, func() bool {
		out := runLSMCTL(t, bin, "get", "--addr", urls["node-c"], "--key", "after-restart")
		return bytes.Contains(out, []byte("found=true")) && bytes.Contains(out, []byte("value=still-replicates"))
	})
}

type startedLSMProcess struct {
	cmd  *exec.Cmd
	logs bytes.Buffer
	done chan error
}

type processConfigOptions struct {
	NodeID        string
	RaftPeers     []string
	ShardReplicas []string
	PeerURLs      map[string]string
	JoinPeerURLs  map[string]string
	Join          bool
}

func buildLSMCTLBinary(t *testing.T, tempDir string) string {
	t.Helper()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	bin := filepath.Join(tempDir, "lsmctl")
	build := exec.Command("go", "build", "-o", bin, "./cmd/lsmctl")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build lsmctl: %v\n%s", err, out)
	}
	return bin
}

func reserveNodeURLs(t *testing.T, peers []string) map[string]string {
	t.Helper()
	urls := make(map[string]string, len(peers))
	for _, nodeID := range peers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %s: %v", nodeID, err)
		}
		urls[nodeID] = "http://" + listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatalf("close listener %s: %v", nodeID, err)
		}
	}
	return urls
}

func writeProcessConfig(t *testing.T, baseDir string, opts processConfigOptions) string {
	t.Helper()
	nodeID := opts.NodeID
	raftPeers := opts.RaftPeers
	shardReplicas := opts.ShardReplicas
	if len(shardReplicas) == 0 {
		shardReplicas = raftPeers
	}
	nodeURL := opts.PeerURLs[nodeID]
	if nodeURL == "" {
		t.Fatalf("missing peer url for %s", nodeID)
	}
	dataDir := filepath.Join(baseDir, nodeID)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}
	path := filepath.Join(baseDir, nodeID+".yaml")
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "data_dir: %q\n", dataDir)
	fmt.Fprintf(&buf, "node_id: %q\n", nodeID)
	fmt.Fprintf(&buf, "cluster_id: %q\n", "cluster-smoke")
	fmt.Fprintf(&buf, "storage_mode: %q\n", "local")
	fmt.Fprintf(&buf, "control_state_path: %q\n", filepath.Join(dataDir, "control_state.json"))
	fmt.Fprintf(&buf, "addr: %q\n", stripHTTP(nodeURL))
	fmt.Fprintf(&buf, "read_timeout: %q\n", "5s")
	fmt.Fprintf(&buf, "write_timeout: %q\n", "5s")
	fmt.Fprintf(&buf, "write_consistency_default: %q\n", "local_committed")
	buf.WriteString("commitlog:\n")
	buf.WriteString("  provider: \"etcd-raft\"\n")
	buf.WriteString("  snapshot_policy:\n")
	buf.WriteString("    applied_entries: 8\n")
	buf.WriteString("    retain_entries: 4\n")
	buf.WriteString("raft:\n")
	buf.WriteString("  peers:\n")
	for _, peer := range raftPeers {
		fmt.Fprintf(&buf, "    - %q\n", peer)
	}
	if opts.Join {
		buf.WriteString("  join: true\n")
	}
	buf.WriteString("  peer_urls:\n")
	for _, peer := range sortedProcessPeers(opts.PeerURLs) {
		fmt.Fprintf(&buf, "    %s: %q\n", peer, opts.PeerURLs[peer])
	}
	if len(opts.JoinPeerURLs) > 0 {
		buf.WriteString("  join_peer_urls:\n")
		for _, peer := range sortedProcessPeers(opts.JoinPeerURLs) {
			fmt.Fprintf(&buf, "    %s: %q\n", peer, opts.JoinPeerURLs[peer])
		}
	}
	buf.WriteString("shards:\n")
	buf.WriteString("  - id: \"users\"\n")
	buf.WriteString("    start_key: \"a\"\n")
	buf.WriteString("    end_key: \"z\"\n")
	buf.WriteString("    replicas:\n")
	for _, peer := range shardReplicas {
		fmt.Fprintf(&buf, "      - %q\n", peer)
	}
	buf.WriteString("    leader: \"node-a\"\n")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write config %s: %v", nodeID, err)
	}
	return path
}

func urlsForPeers(peers []string, urls map[string]string) map[string]string {
	out := make(map[string]string, len(peers))
	for _, peer := range peers {
		out[peer] = urls[peer]
	}
	return out
}

func sortedProcessPeers(urls map[string]string) []string {
	peers := make([]string, 0, len(urls))
	for peer := range urls {
		peers = append(peers, peer)
	}
	sort.Strings(peers)
	return peers
}

func startLSMProcess(t *testing.T, bin string, configPath string) *startedLSMProcess {
	t.Helper()
	cmd := exec.Command(bin, "serve", "--config", configPath)
	proc := &startedLSMProcess{
		cmd:  cmd,
		done: make(chan error, 1),
	}
	cmd.Stdout = &proc.logs
	cmd.Stderr = &proc.logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", configPath, err)
	}
	go func() {
		proc.done <- cmd.Wait()
	}()
	return proc
}

func runLSMCTL(t *testing.T, bin string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("lsmctl %v: %v\n%s", args, err, out)
	}
	return out
}

func (p *startedLSMProcess) stop(t *testing.T) {
	t.Helper()
	if p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	select {
	case err := <-p.done:
		if err != nil {
			t.Logf("process exited after interrupt: %v\n%s", err, p.logs.String())
		}
	case <-time.After(3 * time.Second):
		_ = p.cmd.Process.Kill()
		err := <-p.done
		t.Logf("process killed after shutdown timeout: %v\n%s", err, p.logs.String())
	}
}

func waitHTTPStatus(t *testing.T, url string, want int, timeout time.Duration) {
	t.Helper()
	client := http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == want {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("wait for %s: %v", url, err)
	}
	_ = resp.Body.Close()
	t.Fatalf("wait for %s: got status %d want %d", url, resp.StatusCode, want)
}

func postKVPut(baseURL string, key []byte, value []byte) (int, string, error) {
	body := bytes.NewBufferString(`{"key_base64":"` +
		base64.StdEncoding.EncodeToString(key) +
		`","value_base64":"` +
		base64.StdEncoding.EncodeToString(value) +
		`","consistency":"local_committed"}`)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/kv/put", body)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	var out bytes.Buffer
	_, _ = out.ReadFrom(resp.Body)
	return resp.StatusCode, out.String(), nil
}

func postRaftAdd(baseURL string, nodeID string) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		baseURL+"/cluster/nodes/"+nodeID+"/raft-add",
		bytes.NewBufferString(`{}`),
	)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	var out bytes.Buffer
	_, _ = out.ReadFrom(resp.Body)
	return resp.StatusCode, out.String(), nil
}

func postShardTransferLeader(baseURL string, shardID string, target string) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		baseURL+"/cluster/shards/"+shardID+"/transfer-leader",
		bytes.NewBufferString(`{"target":"`+target+`"}`),
	)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	var out bytes.Buffer
	_, _ = out.ReadFrom(resp.Body)
	return resp.StatusCode, out.String(), nil
}

func getRouteLeader(baseURL string, shardID string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/cluster/routes", nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("routes status %d", resp.StatusCode)
	}
	var out struct {
		Shards []struct {
			ID     string `json:"id"`
			Leader string `json:"leader"`
		} `json:"shards"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	for _, shard := range out.Shards {
		if shard.ID == shardID {
			return shard.Leader, nil
		}
	}
	return "", fmt.Errorf("route shard %q not found", shardID)
}

func getKVValue(baseURL string, key []byte) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		baseURL+"/kv/get?key_base64="+base64.StdEncoding.EncodeToString(key),
		nil,
	)
	if err != nil {
		return nil, false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("get status %d", resp.StatusCode)
	}
	var out struct {
		Found       bool   `json:"found"`
		ValueBase64 string `json:"value_base64"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, false, err
	}
	if !out.Found {
		return nil, false, nil
	}
	value, err := base64.StdEncoding.DecodeString(out.ValueBase64)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func stripHTTP(url string) string {
	const prefix = "http://"
	if len(url) >= len(prefix) && url[:len(prefix)] == prefix {
		return url[len(prefix):]
	}
	return url
}
