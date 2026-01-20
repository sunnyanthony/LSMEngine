package config

import "testing"

func TestOptionsNormalizeDefaults(t *testing.T) {
	opts := DefaultOptions("dir")
	opts.BlockTargetBytes = 0
	opts.BlockMaxBytes = 0
	opts.RestartInterval = 0
	opts.ReadBlockMaxBytes = 0
	opts.ReadBufferMaxBytes = 0
	opts.Compression = ""
	opts.Checksum = ""
	opts.Normalize()

	if opts.BlockTargetBytes == 0 || opts.BlockMaxBytes == 0 {
		t.Fatalf("expected block defaults to be set")
	}
	if opts.BlockMaxBytes < opts.BlockTargetBytes {
		t.Fatalf("expected BlockMaxBytes >= BlockTargetBytes")
	}
	if opts.RestartInterval <= 0 {
		t.Fatalf("expected restart interval default")
	}
	if opts.ReadBlockMaxBytes <= 0 || opts.ReadBufferMaxBytes <= 0 {
		t.Fatalf("expected read buffer defaults")
	}
	if opts.Compression != CompressionSnappy {
		t.Fatalf("expected compression default snappy, got %q", opts.Compression)
	}
	if opts.Checksum != ChecksumCRC32C {
		t.Fatalf("expected checksum default crc32c, got %q", opts.Checksum)
	}
}

func TestOptionsNormalizeCacheSplit(t *testing.T) {
	opts := DefaultOptions("dir")
	opts.BlockCacheBytes = 1024
	opts.IndexCacheBytes = 0
	opts.FilterCacheBytes = 0
	opts.Normalize()
	if opts.IndexCacheBytes != opts.BlockCacheBytes/8 {
		t.Fatalf("expected index cache split, got %d", opts.IndexCacheBytes)
	}
	if opts.FilterCacheBytes != opts.BlockCacheBytes/8 {
		t.Fatalf("expected filter cache split, got %d", opts.FilterCacheBytes)
	}
}

func TestOptionsNormalizeReadBuffer(t *testing.T) {
	opts := DefaultOptions("dir")
	opts.BlockMaxBytes = 128
	opts.ReadBlockMaxBytes = 0
	opts.ReadBufferMaxBytes = 0
	opts.Normalize()
	if opts.ReadBlockMaxBytes != opts.BlockMaxBytes*4 {
		t.Fatalf("expected read block max to scale, got %d", opts.ReadBlockMaxBytes)
	}
	if opts.ReadBufferMaxBytes != opts.ReadBlockMaxBytes {
		t.Fatalf("expected read buffer to match read block max, got %d", opts.ReadBufferMaxBytes)
	}
}

func TestOptionsValidateRejectsUnknownValues(t *testing.T) {
	opts := DefaultOptions("dir")
	opts.Compression = "nope"
	if err := opts.Validate(); err == nil {
		t.Fatalf("expected compression validation error")
	}
	opts = DefaultOptions("dir")
	opts.Checksum = "nope"
	if err := opts.Validate(); err == nil {
		t.Fatalf("expected checksum validation error")
	}
	opts = DefaultOptions("dir")
	opts.CorruptionPolicy = "nope"
	if err := opts.Validate(); err == nil {
		t.Fatalf("expected corruption policy validation error")
	}
}
