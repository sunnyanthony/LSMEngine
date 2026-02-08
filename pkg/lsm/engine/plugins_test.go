package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"lsmengine/pkg/lsm/errs"
)

type fakePlugin struct {
	spec    PluginSpec
	started atomic.Bool
	stopped atomic.Bool
}

func (p *fakePlugin) Spec() PluginSpec {
	return p.spec
}

func (p *fakePlugin) Start(ctx context.Context, host PluginHost) error {
	p.started.Store(true)
	return nil
}

func (p *fakePlugin) Stop(ctx context.Context) error {
	p.stopped.Store(true)
	return nil
}

func (p *fakePlugin) Invoke(ctx context.Context, req PluginRequest) (PluginResponse, error) {
	if req.Action == "fail" {
		return PluginResponse{}, errors.New("invoke failed")
	}
	return PluginResponse{Payload: []byte(`{"ok":true}`)}, nil
}

func TestPluginManagerValidation(t *testing.T) {
	_, err := newPluginManager([]Plugin{nil})
	if !errors.Is(err, errs.ErrPluginInvalid) {
		t.Fatalf("expected ErrPluginInvalid, got %v", err)
	}
	_, err = newPluginManager([]Plugin{
		&fakePlugin{spec: PluginSpec{Name: "dup"}},
		&fakePlugin{spec: PluginSpec{Name: "dup"}},
	})
	if !errors.Is(err, errs.ErrPluginDuplicate) {
		t.Fatalf("expected ErrPluginDuplicate, got %v", err)
	}
}

func TestPluginLifecycleAndInvoke(t *testing.T) {
	p := &fakePlugin{
		spec: PluginSpec{
			Name:  "vector-demo",
			Kinds: []PluginKind{PluginKindVector},
		},
	}
	store, err := New(Options{
		DataDir: t.TempDir(),
		Plugins: []Plugin{p},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if !p.started.Load() {
		t.Fatalf("expected plugin start")
	}
	specs := store.Plugins()
	if len(specs) != 1 || specs[0].Name != "vector-demo" {
		t.Fatalf("unexpected plugin specs: %+v", specs)
	}
	resp, err := store.InvokePlugin(context.Background(), "vector-demo", PluginRequest{Action: "search"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if string(resp.Payload) != `{"ok":true}` {
		t.Fatalf("unexpected payload: %s", string(resp.Payload))
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !p.stopped.Load() {
		t.Fatalf("expected plugin stop")
	}
}

func TestInvokePluginNotFound(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()
	_, err = store.InvokePlugin(context.Background(), "missing", PluginRequest{})
	if !errors.Is(err, errs.ErrPluginNotFound) {
		t.Fatalf("expected ErrPluginNotFound, got %v", err)
	}
}
