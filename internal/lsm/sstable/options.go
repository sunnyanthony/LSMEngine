package sstable

import "fmt"

type Compression string

const (
	CompressionNone   Compression = "none"
	CompressionSnappy Compression = "snappy"
)

type Checksum string

const (
	ChecksumCRC32C Checksum = "crc32c"
)

type Options struct {
	Dir              string
	BlockTargetBytes int
	BlockMaxBytes    int
	RestartInterval  int
	Compression      Compression
	BloomBitsPerKey  int
	BlockCacheBytes  int64
	PrefetchBlocks   int
	Checksum         Checksum
}

func DefaultOptions(dir string) Options {
	return Options{
		Dir:              dir,
		BlockTargetBytes: 64 * 1024,
		BlockMaxBytes:    256 * 1024,
		RestartInterval:  16,
		Compression:      CompressionSnappy,
		BloomBitsPerKey:  10,
		BlockCacheBytes:  64 * 1024 * 1024,
		PrefetchBlocks:   2,
		Checksum:         ChecksumCRC32C,
	}
}

func (o *Options) normalize() {
	if o.BlockTargetBytes <= 0 {
		o.BlockTargetBytes = 64 * 1024
	}
	if o.BlockMaxBytes <= 0 {
		o.BlockMaxBytes = 256 * 1024
	}
	if o.BlockMaxBytes < o.BlockTargetBytes {
		o.BlockMaxBytes = o.BlockTargetBytes
	}
	if o.RestartInterval <= 0 {
		o.RestartInterval = 16
	}
	if o.Compression == "" {
		o.Compression = CompressionSnappy
	}
	if o.Checksum == "" {
		o.Checksum = ChecksumCRC32C
	}
	if o.PrefetchBlocks < 0 {
		o.PrefetchBlocks = 0
	}
}

func (o *Options) validate() error {
	switch o.Compression {
	case CompressionNone, CompressionSnappy:
	default:
		return fmt.Errorf("unsupported compression %q", o.Compression)
	}
	switch o.Checksum {
	case ChecksumCRC32C:
	default:
		return fmt.Errorf("unsupported checksum %q", o.Checksum)
	}
	return nil
}
