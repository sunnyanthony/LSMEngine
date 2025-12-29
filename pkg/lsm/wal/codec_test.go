package wal

import (
	"bufio"
	"bytes"
	"errors"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
	"testing"
)

func TestCodecRoundTrip(t *testing.T) {
	entry := types.Entry{
		Key:       "alpha",
		Value:     []byte("value-123"),
		Tombstone: false,
		Seq:       42,
	}
	rec := encodeEntry(entry)
	entries, err := decodeRecords(rec)
	if err != nil {
		t.Fatalf("decodeRecords: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 record")
	}
	dec := entries[0]
	if dec.Key != entry.Key || dec.Seq != entry.Seq || string(dec.Value) != string(entry.Value) || dec.Tombstone != entry.Tombstone {
		t.Fatalf("round-trip mismatch: got %+v want %+v", dec, entry)
	}
}

func TestCodecCorruptChecksum(t *testing.T) {
	entry := types.Entry{Key: "k", Value: []byte("v"), Seq: 1}
	rec := encodeEntry(entry)
	rec[len(rec)-1] ^= 0xFF // flip checksum byte

	_, err := decodeRecords(rec)
	if err == nil || !errors.Is(err, errs.ErrWALCorrupt) {
		t.Fatalf("expected ErrCorrupt, got err=%v", err)
	}
}

func TestCodecUnknownVersion(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	if _, err := writeSegmentHeader(buf, 64*1024, 1); err != nil {
		t.Fatalf("write segment header: %v", err)
	}
	raw := buf.Bytes()
	raw[4] = 0xFF // corrupt version
	_, err := readSegmentHeader(bytes.NewReader(raw))
	if err == nil || !errors.Is(err, errs.ErrWALCorrupt) {
		t.Fatalf("expected ErrCorrupt for unknown version, got err=%v", err)
	}
}

func TestCodecResyncFindsNextMagic(t *testing.T) {
	rb1 := newRecordBuffer(types.Entry{Key: "a", Value: []byte("1"), Seq: 1})
	rb2 := newRecordBuffer(types.Entry{Key: "b", Value: []byte("2"), Seq: 2})
	buf := bytes.NewBuffer(nil)
	_, _ = writeBlock(buf, []recordBuffer{rb1})
	_, _ = writeBlock(buf, []recordBuffer{rb2})
	data := buf.Bytes()
	// Corrupt the first block CRC.
	data[8] ^= 0xFF

	r := bufio.NewReader(bytes.NewReader(data))
	_, _, err := decodeBlock(r, 64*1024)
	if err == nil || !errors.Is(err, errs.ErrWALCorrupt) {
		t.Fatalf("expected first block to be corrupt, err=%v", err)
	}
	found, err := resyncBlock(r)
	if err != nil || !found {
		t.Fatalf("expected to resync to next magic, err=%v found=%v", err, found)
	}
	payload, ok, err := decodeBlockAfterMagic(r, 64*1024)
	if err != nil || !ok {
		t.Fatalf("expected second block after resync, err=%v ok=%v", err, ok)
	}
	entries, err := decodeRecords(payload)
	if err != nil || len(entries) != 1 || entries[0].Key != "b" {
		t.Fatalf("expected second record after resync, got %+v err=%v", entries, err)
	}
}

func TestDecodeBlockPayloadExceedsCap(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	_, _ = writeBlock(buf, []recordBuffer{newRecordBuffer(types.Entry{Key: "a", Value: []byte("1"), Seq: 1})})
	r := bufio.NewReader(bytes.NewReader(buf.Bytes()))
	_, _, err := decodeBlock(r, 1)
	if err == nil || !errors.Is(err, errs.ErrWALCorrupt) {
		t.Fatalf("expected payload cap corruption, got err=%v", err)
	}
}
