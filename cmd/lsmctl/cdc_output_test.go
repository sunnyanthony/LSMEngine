package main

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

func TestCDCTextEscapesBinaryData(t *testing.T) {
	var out bytes.Buffer
	writeCDCEvent(&out, cdcEventResult{
		Offset: 1, Operation: "put",
		KeyBase64:   base64.StdEncoding.EncodeToString([]byte("key\nforged=entry")),
		ValueBase64: base64.StdEncoding.EncodeToString([]byte{0x1b, '[', '2', 'J', 0xff}),
	})
	text := out.String()
	if strings.Count(text, "\n") != 1 || strings.ContainsRune(text, '\x1b') {
		t.Fatalf("unsafe multiline/control output: %q", text)
	}
	if !strings.Contains(text, `key="key\nforged=entry"`) || !strings.Contains(text, `value="\x1b[2J\xff"`) {
		t.Fatalf("binary values not losslessly escaped: %q", text)
	}
}
