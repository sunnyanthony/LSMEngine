package sstable

import (
	"lsmengine/internal/lsm/sstable/block"
	"lsmengine/internal/lsm/sstable/config"
)

type Options = config.Options
type Compression = config.Compression
type Checksum = config.Checksum
type CorruptionPolicy = config.CorruptionPolicy
type FlowObserver = config.FlowObserver
type FlowEvent = config.FlowEvent
type FlowMetrics = config.FlowMetrics
type MetricsSnapshot = config.MetricsSnapshot
type MetricsObserver = config.MetricsObserver
type PolicySnapshot = config.PolicySnapshot
type EntryView = block.EntryView

const (
	CompressionNone   = config.CompressionNone
	CompressionSnappy = config.CompressionSnappy

	ChecksumCRC32C = config.ChecksumCRC32C

	CorruptionFailFast  = config.CorruptionFailFast
	CorruptionSkipBlock = config.CorruptionSkipBlock
	CorruptionDropTable = config.CorruptionDropTable
)

func DefaultOptions(dir string) Options {
	return config.DefaultOptions(dir)
}

func NewMetricsObserver(target *FlowMetrics) *MetricsObserver {
	return config.NewMetricsObserver(target)
}
