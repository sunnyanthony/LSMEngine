//go:build !test

package runtime

func currentHooks() *applyHooks {
	return nil
}
