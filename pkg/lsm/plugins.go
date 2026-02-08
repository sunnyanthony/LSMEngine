package lsm

import (
	"context"

	"lsmengine/pkg/lsm/engine"
)

type PluginKind = engine.PluginKind
type PluginSpec = engine.PluginSpec
type PluginRequest = engine.PluginRequest
type PluginResponse = engine.PluginResponse
type Plugin = engine.Plugin
type PluginLifecycle = engine.PluginLifecycle
type PluginHost = engine.PluginHost

const (
	PluginKindDocument = engine.PluginKindDocument
	PluginKindColumn   = engine.PluginKindColumn
	PluginKindVector   = engine.PluginKindVector
)

// PluginProvider exposes plugin discovery and invocation.
type PluginProvider interface {
	Plugins() []PluginSpec
	InvokePlugin(ctx context.Context, name string, req PluginRequest) (PluginResponse, error)
}
