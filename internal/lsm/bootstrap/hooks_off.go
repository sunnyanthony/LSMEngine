//go:build !test

package bootstrap

func currentHooks() *recoveryHooks {
	return nil
}
