package codec

import (
	"bufio"
	"bytes"
	"errors"
	"testing"

	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

func TestCodecRoundTrip(t *testing.T) {
	entry := types.Entry{
		Key:       []byte("alpha"),
		Value:     []byte("value-123"),
		Tombstone: false,
		Seq:       42,
	}
	rec := EncodeEntry(entry)
	entries, err := DecodeRecords(rec)
	if err != nil {
		t.Fatalf("decodeRecords: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 record")
	}
	dec := entries[0]
	if !bytes.Equal(dec.Key, entry.Key) || dec.Seq != entry.Seq || !bytes.Equal(dec.Value, entry.Value) || dec.Tombstone != entry.Tombstone {
		t.Fatalf("round-trip mismatch: got %+v want %+v", dec, entry)
	}
}

func TestCodecCorruptChecksum(t *testing.T) {
	entry := types.Entry{Key: []byte("k"), Value: []byte("v"), Seq: 1}
	rec := EncodeEntry(entry)
	rec[len(rec)-1] ^= 0xFF // flip checksum byte

	_, err := DecodeRecords(rec)
	if err == nil || !errors.Is(err, errs.ErrWALCorrupt) {
		t.Fatalf("expected ErrCorrupt, got err=%v", err)
	}
}

func TestCodecUnknownVersion(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	if _, err := WriteSegmentHeader(buf, 64*1024, 1); err != nil {
		t.Fatalf("write segment header: %v", err)
	}
	raw := buf.Bytes()
	raw[4] = 0xFF // corrupt version
	_, err := ReadSegmentHeader(bytes.NewReader(raw))
	if err == nil || !errors.Is(err, errs.ErrWALCorrupt) {
		t.Fatalf("expected ErrCorrupt for unknown version, got err=%v", err)
	}
}

func TestCodecResyncFindsNextMagic(t *testing.T) {
	rb1 := NewRecordBuffer(types.Entry{Key: []byte("a"), Value: []byte("1"), Seq: 1})
	rb2 := NewRecordBuffer(types.Entry{Key: []byte("b"), Value: []byte("2"), Seq: 2})
	buf := bytes.NewBuffer(nil)
	_, _ = WriteBlock(buf, []RecordBuffer{rb1})
	_, _ = WriteBlock(buf, []RecordBuffer{rb2})
	data := buf.Bytes()
	// Corrupt the first block CRC.
	data[8] ^= 0xFF

	r := bufio.NewReader(bytes.NewReader(data))
	_, _, err := DecodeBlock(r, 64*1024)
	if err == nil || !errors.Is(err, errs.ErrWALCorrupt) {
		t.Fatalf("expected first block to be corrupt, err=%v", err)
	}
	found, err := ResyncBlock(r)
	if err != nil || !found {
		t.Fatalf("expected to resync to next magic, err=%v found=%v", err, found)
	}
	payload, ok, err := DecodeBlockAfterMagic(r, 64*1024)
	if err != nil || !ok {
		t.Fatalf("expected second block after resync, err=%v ok=%v", err, ok)
	}
	entries, err := DecodeRecords(payload)
	if err != nil || len(entries) != 1 || !bytes.Equal(entries[0].Key, []byte("b")) {
		t.Fatalf("expected second record after resync, got %+v err=%v", entries, err)
	}
}

func TestDecodeBlockPayloadExceedsCap(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	_, _ = WriteBlock(buf, []RecordBuffer{NewRecordBuffer(types.Entry{Key: []byte("a"), Value: []byte("1"), Seq: 1})})
	r := bufio.NewReader(bytes.NewReader(buf.Bytes()))
	_, _, err := DecodeBlock(r, 1)
	if err == nil || !errors.Is(err, errs.ErrWALCorrupt) {
		t.Fatalf("expected payload cap corruption, got err=%v", err)
	}
}
