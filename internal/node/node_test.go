package node_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/test/bufconn"

	"github.com/rakeshlakshmanan/ringcache/internal/clock"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/node"
)

// start runs a node on an in-process listener and returns it once it is
// serving. It is stopped when the test ends.
func start(t *testing.T) *node.Node {
	t.Helper()

	n, err := node.New(config.Config{
		NodeID:        "node-1",
		Advertise:     "node-1:7000",
		MetricsListen: "127.0.0.1:0",
	}, node.Options{
		Listener: bufconn.Listen(1 << 20),
		Clock:    clock.NewFake(),
	})
	if err != nil {
		t.Fatalf("node.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Start(ctx) }()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("node.Start: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("node.Start did not return after the context was cancelled")
		}
		_ = n.Close()
	})

	waitHealthy(t, n)
	return n
}

// waitHealthy polls /healthz until it answers 200. Start brings the
// listeners up in goroutines, so the first request can arrive first.
func waitHealthy(t *testing.T, n *node.Node) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		code, _ := get(t, n, "/healthz")
		if code == http.StatusOK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("/healthz never returned 200; last status %d", code)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func get(t *testing.T, n *node.Node, path string) (int, string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := "http://" + n.MetricsAddr().String() + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body)
}

func TestNode_Healthz(t *testing.T) {
	n := start(t)
	if code, body := get(t, n, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d %q, want 200", code, body)
	}
}

func TestNode_MetricsEndpoint(t *testing.T) {
	n := start(t)

	code, body := get(t, n, "/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", code)
	}
	// The endpoint serves this node's registry, not the global default
	// one, so several nodes in one process report separate numbers.
	if !strings.Contains(body, `ringcache_store_keys{node="node-1"}`) {
		t.Errorf("/metrics does not carry this node's store gauge:\n%s", body)
	}
	if strings.Contains(body, "go_goroutines") {
		t.Error("/metrics carries the default registry's process collectors, so the registry is not per-node")
	}
}

// Cancelling the context has to stop the servers and return, rather
// than leaving the process holding its ports.
func TestNode_ShutdownOnCancel(t *testing.T) {
	n, err := node.New(config.Config{
		NodeID:        "node-1",
		Advertise:     "node-1:7000",
		MetricsListen: "127.0.0.1:0",
	}, node.Options{Listener: bufconn.Listen(1 << 20), Clock: clock.NewFake()})
	if err != nil {
		t.Fatalf("node.New: %v", err)
	}
	defer n.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Start(ctx) }()

	waitHealthy(t, n)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start returned %v, want nil on a clean shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after the context was cancelled")
	}

	// Close after Start has already shut down must not panic or block.
	if err := n.Close(); err != nil {
		t.Errorf("Close after shutdown: %v", err)
	}
}

// Two nodes must be able to run in one process: the test cluster in
// phase 6 and the benchmarks both depend on it, which is why nothing
// here uses package-level state.
func TestNode_TwoInOneProcess(t *testing.T) {
	a, b := start(t), start(t)
	if a.MetricsAddr().String() == b.MetricsAddr().String() {
		t.Fatal("both nodes bound the same metrics address")
	}

	a.Store().SetLocal("k", []byte("v"), 0)
	if a.Store().Len() != 1 || b.Store().Len() != 0 {
		t.Errorf("stores are shared: a has %d keys, b has %d", a.Store().Len(), b.Store().Len())
	}
}

// A port already in use has to fail at New, before Start, so the binary
// exits non-zero instead of appearing to run.
func TestNode_ListenConflict(t *testing.T) {
	first := start(t)

	_, err := node.New(config.Config{
		NodeID:        "node-2",
		Advertise:     "node-2:7000",
		MetricsListen: first.MetricsAddr().String(),
	}, node.Options{Listener: bufconn.Listen(1 << 20), Clock: clock.NewFake()})
	if err == nil {
		t.Fatal("node.New succeeded on an address already in use")
	}
	if !strings.Contains(err.Error(), first.MetricsAddr().String()) {
		t.Errorf("error %v does not name the address it could not bind", err)
	}
}
