package engine

import "lsmengine/internal/lsm/manifest"

func (l *LSM) updateManifest(mutator func(manifest.Manifest) manifest.Manifest) error {
	if l == nil || l.manifest == nil || mutator == nil {
		return nil
	}
	if updater, ok := l.manifest.(manifest.UpdateStore); ok {
		return updater.Update(mutator)
	}
	m, err := l.manifest.Load()
	if err != nil {
		return err
	}
	m = mutator(m)
	return l.manifest.Save(m)
}
