package engine

import (
	"context"

	"lsmengine/pkg/lsm/errs"
)

// Plugins returns the list of registered plugins.
func (l *LSM) Plugins() []PluginSpec {
	if l == nil || l.plugins == nil {
		return nil
	}
	return l.plugins.specs()
}

// InvokePlugin routes a typed request to a named plugin.
func (l *LSM) InvokePlugin(ctx context.Context, name string, req PluginRequest) (PluginResponse, error) {
	if l == nil || l.plugins == nil {
		return PluginResponse{}, errs.ErrPluginNotFound
	}
	return l.plugins.invoke(ctx, name, req)
}
