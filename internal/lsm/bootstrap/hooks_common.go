package bootstrap

import "lsmengine/internal/lsm/manifest"

type recoveryHooks struct {
	BeforeFallbackScan func()
	AfterFallbackScan  func(manifest.Manifest, error)
	BeforeManifestSave func(manifest.Manifest)
	AfterManifestSave  func(manifest.Manifest, error)
}
