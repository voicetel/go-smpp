// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pdutext

import (
	"bytes"
	"testing"
)

func TestLatin1Encoder(t *testing.T) {
	want := []byte("Ol\xe1 mund\xe3o")
	text := []byte("Olá mundão")
	s := Latin1(text)
	if s.Type() != 0x03 {
		t.Fatalf("Unexpected data type; want 0x03, have %d", s.Type())
	}
	have := s.Encode()
	if !bytes.Equal(want, have) {
		t.Fatalf("Unexpected text; want %q, have %q", want, have)
	}
}

func TestLatin1Decoder(t *testing.T) {
	want := []byte("Olá mundão")
	text := []byte("Ol\xe1 mund\xe3o")
	s := Latin1(text)
	if s.Type() != 0x03 {
		t.Fatalf("Unexpected data type; want 0x03, have %d", s.Type())
	}
	have := s.Decode()
	if !bytes.Equal(want, have) {
		t.Fatalf("Unexpected text; want %q, have %q", want, have)
	}
}

// TestLatin1_ExactISO8859_1 pins RFC-1345 / ISO_8859-1:1987 parity: the
// Windows-1252 printable overlay at 0x80-0x9F is NOT part of ISO-8859-1, so a
// CP1252-only code point (euro U+20AC) must NOT encode to byte 0x80, and the C1
// control U+0080 maps to byte 0x80 (which CP1252 cannot represent).
func TestLatin1_ExactISO8859_1(t *testing.T) {
	if got := Latin1("€").Encode(); bytes.Equal(got, []byte{0x80}) {
		t.Fatal("euro encoded to 0x80 — Windows-1252 overlay leaked into Latin1")
	}
	if got := Latin1("\u0080").Encode(); !bytes.Equal(got, []byte{0x80}) {
		t.Fatalf("U+0080 must map to 0x80 in ISO-8859-1; got % X", got)
	}
	if got := Latin1("ñ").Encode(); !bytes.Equal(got, []byte{0xf1}) {
		t.Fatalf("n-tilde must map to 0xF1; got % X", got)
	}
}
