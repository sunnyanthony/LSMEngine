package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

// PluginKind classifies plugin capability families.
type PluginKind string

const (
	PluginKindDocument PluginKind = "document"
	PluginKindColumn   PluginKind = "column"
	PluginKindVector   PluginKind = "vector"
)

// PluginSpec describes a plugin for discovery and control-plane introspection.
type PluginSpec struct {
	Name        string       `json:"name"`
	Version     string       `json:"version,omitempty"`
	Description string       `json:"description,omitempty"`
	Kinds       []PluginKind `json:"kinds,omitempty"`
}

// PluginRequest is a generic invocation payload.
type PluginRequest struct {
	Action   string            `json:"action"`
	Payload  json.RawMessage   `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// PluginResponse is a generic invocation result.
type PluginResponse struct {
	Payload  json.RawMessage   `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Plugin is the extension point for domain-specific features (document/column/vector).
type Plugin interface {
	Spec() PluginSpec
	Invoke(ctx context.Context, req PluginRequest) (PluginResponse, error)
}

// PluginLifecycle is optional and allows startup/shutdown hooks.
type PluginLifecycle interface {
	Start(ctx context.Context, host PluginHost) error
	Stop(ctx context.Context) error
}

// PluginHost exposes stable engine operations to plugins.
type PluginHost interface {
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Get(key []byte) (types.Entry, bool)
	Snapshot() *Snapshot
}

type pluginManager struct {
	order []string
	byKey map[string]Plugin
	mu    sync.RWMutex
}

func newPluginManager(plugins []Plugin) (*pluginManager, error) {
	if len(plugins) == 0 {
		return nil, nil
	}
	m := &pluginManager{
		order: make([]string, 0, len(plugins)),
		byKey: make(map[string]Plugin, len(plugins)),
	}
	for _, p := range plugins {
		if p == nil {
			return nil, errs.ErrPluginInvalid
		}
		spec := p.Spec()
		name := strings.TrimSpace(spec.Name)
		if name == "" {
			return nil, errs.ErrPluginInvalid
		}
		if _, ok := m.byKey[name]; ok {
			return nil, errs.ErrPluginDuplicate
		}
		m.order = append(m.order, name)
		m.byKey[name] = p
	}
	return m, nil
}

func (m *pluginManager) start(ctx context.Context, host PluginHost) error {
	if m == nil {
		return nil
	}
	started := make([]PluginLifecycle, 0, len(m.order))
	for _, name := range m.order {
		p := m.byKey[name]
		lc, ok := p.(PluginLifecycle)
		if !ok {
			continue
		}
		if err := lc.Start(ctx, host); err != nil {
			for i := len(started) - 1; i >= 0; i-- {
				_ = started[i].Stop(context.Background())
			}
			return err
		}
		started = append(started, lc)
	}
	return nil
}

func (m *pluginManager) stop(ctx context.Context) error {
	if m == nil {
		return nil
	}
	var joined error
	for i := len(m.order) - 1; i >= 0; i-- {
		p := m.byKey[m.order[i]]
		lc, ok := p.(PluginLifecycle)
		if !ok {
			continue
		}
		if err := lc.Stop(ctx); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

func (m *pluginManager) specs() []PluginSpec {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]PluginSpec, 0, len(m.order))
	for _, name := range m.order {
		spec := m.byKey[name].Spec()
		spec.Kinds = append([]PluginKind(nil), spec.Kinds...)
		out = append(out, spec)
	}
	return out
}

func (m *pluginManager) invoke(ctx context.Context, name string, req PluginRequest) (PluginResponse, error) {
	if m == nil {
		return PluginResponse{}, errs.ErrPluginNotFound
	}
	m.mu.RLock()
	p := m.byKey[name]
	m.mu.RUnlock()
	if p == nil {
		return PluginResponse{}, errs.ErrPluginNotFound
	}
	return p.Invoke(ctx, req)
}
