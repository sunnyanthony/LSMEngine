// Monitoring interfaces for stats and health.

package lsm

// StatsProvider exposes stats and health snapshots for monitoring.
type StatsProvider interface {
	Stats() Stats
	Health() Health
}
