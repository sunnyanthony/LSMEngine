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
	"strings"
	"testing"
	"time"
)

func TestEtcdRaftThreeProcessSmoke(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	tempDir := t.TempDir()
	bin := filepath.Join(tempDir, "lsmctl")
	build := exec.Command("go", "build", "-o", bin, "./cmd/lsmctl")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build lsmctl: %v\n%s", err, out)
	}

	peers := []string{"node-a", "node-b", "node-c"}
	urls := reserveNodeURLs(t, peers)
	var processes []*startedLSMProcess
	for _, nodeID := range peers {
		configPath := writeProcessConfig(t, tempDir, nodeID, peers, urls)
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
	writeNode := eventuallyPostKVPut(t, urls, peers, key, value, 10*time.Second)
	t.Logf("committed multi-process write through %s", writeNode)

	for _, nodeID := range peers {
		nodeID := nodeID
		t.Run(nodeID, func(t *testing.T) {
			eventually(t, 10*time.Second, func() bool {
				got, ok, err := getKVValue(urls[nodeID], key)
				return err == nil && ok && bytes.Equal(got, value)
			})
		})
	}

	cliWriteNode := eventuallyRunLSMCTLWrite(t, bin, urls, peers, "put", "cli", "value", 10*time.Second)
	t.Logf("committed lsmctl put through %s", cliWriteNode)
	eventually(t, 5*time.Second, func() bool {
		out := runLSMCTL(t, bin, "get", "--addr", urls["node-b"], "--key", "cli")
		return bytes.Contains(out, []byte("found=true")) && bytes.Contains(out, []byte("value=value"))
	})
	cliDeleteNode := eventuallyRunLSMCTLWrite(t, bin, urls, peers, "delete", "cli", "", 10*time.Second)
	t.Logf("committed lsmctl delete through %s", cliDeleteNode)
	eventually(t, 5*time.Second, func() bool {
		out := runLSMCTL(t, bin, "get", "--addr", urls["node-c"], "--key", "cli")
		return bytes.Contains(out, []byte("found=false"))
	})
}

type startedLSMProcess struct {
	cmd  *exec.Cmd
	logs bytes.Buffer
	done chan error
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

func writeProcessConfig(t *testing.T, baseDir string, nodeID string, peers []string, urls map[string]string) string {
	t.Helper()
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
	fmt.Fprintf(&buf, "addr: %q\n", stripHTTP(urls[nodeID]))
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
	for _, peer := range peers {
		fmt.Fprintf(&buf, "    - %q\n", peer)
	}
	buf.WriteString("  peer_urls:\n")
	for _, peer := range peers {
		fmt.Fprintf(&buf, "    %s: %q\n", peer, urls[peer])
	}
	buf.WriteString("shards:\n")
	buf.WriteString("  - id: \"users\"\n")
	buf.WriteString("    start_key: \"a\"\n")
	buf.WriteString("    end_key: \"z\"\n")
	buf.WriteString("    replicas:\n")
	for _, peer := range peers {
		fmt.Fprintf(&buf, "      - %q\n", peer)
	}
	buf.WriteString("    leader: \"node-a\"\n")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write config %s: %v", nodeID, err)
	}
	return path
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

func eventuallyPostKVPut(t *testing.T, urls map[string]string, peers []string, key []byte, value []byte, timeout time.Duration) string {
	t.Helper()
	last := make(map[string]string, len(peers))
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, nodeID := range peers {
			status, body, err := postKVPut(urls[nodeID], key, value)
			if err != nil {
				last[nodeID] = err.Error()
				continue
			}
			if status == http.StatusOK {
				return nodeID
			}
			last[nodeID] = fmt.Sprintf("status=%d body=%s", status, strings.TrimSpace(body))
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("no raft node accepted local_committed write within %s: %s", timeout, formatNodeAttempts(peers, last))
	return ""
}

func formatNodeAttempts(peers []string, attempts map[string]string) string {
	parts := make([]string, 0, len(peers))
	for _, nodeID := range peers {
		msg := attempts[nodeID]
		if msg == "" {
			msg = "not attempted"
		}
		parts = append(parts, nodeID+"="+msg)
	}
	return strings.Join(parts, "; ")
}

func eventuallyRunLSMCTLWrite(t *testing.T, bin string, urls map[string]string, peers []string, command string, key string, value string, timeout time.Duration) string {
	t.Helper()
	last := make(map[string]string, len(peers))
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, nodeID := range peers {
			args := []string{command, "--addr", urls[nodeID], "--key", key}
			if command == "put" {
				args = append(args, "--value", value)
			}
			out, err := runLSMCTLResult(t, bin, args...)
			if err == nil {
				return nodeID
			}
			last[nodeID] = fmt.Sprintf("%v: %s", err, strings.TrimSpace(string(out)))
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("no raft node accepted lsmctl %s within %s: %s", command, timeout, formatNodeAttempts(peers, last))
	return ""
}

func runLSMCTL(t *testing.T, bin string, args ...string) []byte {
	t.Helper()
	out, err := runLSMCTLResult(t, bin, args...)
	if err != nil {
		t.Fatalf("lsmctl %v: %v\n%s", args, err, out)
	}
	return out
}

func runLSMCTLResult(t *testing.T, bin string, args ...string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	out, err := cmd.CombinedOutput()
	return out, err
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
