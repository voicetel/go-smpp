// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pdufield

import (
	"bytes"
	"strconv"
	"testing"
)

func TestFixed(t *testing.T) {
	f := &Fixed{Data: 0x34}
	if f.Len() != 1 {
		t.Fatalf("unexpected len: want 1, have %d", f.Len())
	}
	if v, ok := f.Raw().(uint8); !ok {
		t.Fatalf("unexpected type: want uint8, have %#v", v)
	}
	ws := strconv.Itoa(0x34)
	if v := f.String(); v != string(ws) {
		t.Fatalf("unexpected string: want %q, have %q", ws, v)
	}
	wb := []byte{0x34}
	if v := f.Bytes(); !bytes.Equal(wb, v) {
		t.Fatalf("unexpected bytes: want %q, have %q", wb, v)
	}
	var b bytes.Buffer
	if err := f.SerializeTo(&b); err != nil {
		t.Fatalf("serialization failed: %s", err)
	}
	if v := b.Bytes(); !bytes.Equal(wb, v) {
		t.Fatalf("unexpected serialized bytes: want %q, have %q", wb, v)
	}
}

func TestVariable(t *testing.T) {
	want := []byte("foobar")
	f := &Variable{Data: want}
	lw := len(want) + 1
	if f.Len() != lw {
		t.Fatalf("unexpected len: want %d, have %d", lw, f.Len())
	}
	if v, ok := f.Raw().([]byte); !ok {
		t.Fatalf("unexpected type: want []byte, have %#v", v)
	}
	if v := f.String(); v != string(want) {
		t.Fatalf("unexpected string: want %q have %q", want, v)
	}
	want = []byte("foobar\x00")
	if v := f.Bytes(); !bytes.Equal(want, v) {
		t.Fatalf("unexpected bytes: want %q, have %q", want, v)
	}
	var b bytes.Buffer
	if err := f.SerializeTo(&b); err != nil {
		t.Fatalf("serialization failed: %s", err)
	}
	if v := b.Bytes(); !bytes.Equal(want, v) {
		t.Fatalf("unexpected serialized bytes: want %q, have %q", want, v)
	}
}

// TestVariableNoMutation guards against the original Bytes() that did
// append(v.Data, 0x00) and could silently mutate or alias the caller's
// slice when there was spare capacity.
func TestVariableNoMutation(t *testing.T) {
	src := make([]byte, 6, 16) // unterminated, plenty of spare cap
	copy(src, "foobar")
	f := &Variable{Data: src}

	first := f.Bytes()
	second := f.Bytes()

	if !bytes.Equal(first, second) {
		t.Fatalf("Bytes not idempotent: %q vs %q", first, second)
	}
	if &first[0] == &src[0] || &second[0] == &src[0] {
		t.Fatal("Bytes returned slice aliasing Data")
	}
	if !bytes.Equal(src, []byte("foobar")) {
		t.Fatalf("Data mutated: have %q", src)
	}
	if len(src) != 6 {
		t.Fatalf("Data length grew: have %d", len(src))
	}
}

// TestVariableLenMatchesBytes asserts Len reports the same number of
// bytes that Bytes (and SerializeTo) actually emit, for both
// terminated and unterminated input.
func TestVariableLenMatchesBytes(t *testing.T) {
	cases := [][]byte{
		nil,
		[]byte(""),
		[]byte("foo"),
		[]byte("foo\x00"),
		[]byte("\x00"),
	}
	for _, in := range cases {
		f := &Variable{Data: in}
		if f.Len() != len(f.Bytes()) {
			t.Fatalf("Len/Bytes mismatch for %q: Len=%d len(Bytes)=%d",
				in, f.Len(), len(f.Bytes()))
		}
	}
}

func TestSM(t *testing.T) {
	want := []byte("foobar")
	f := &SM{Data: want}
	if f.Len() != len(want) {
		t.Fatalf("unexpected len: want %d, have %d", len(want), f.Len())
	}
	if v, ok := f.Raw().([]byte); !ok {
		t.Fatalf("unexpected type: want []byte, have %#v", v)
	}
	if v := f.String(); v != string(want) {
		t.Fatalf("unexpected string: want %q have %q", want, v)
	}
	if v := f.Bytes(); !bytes.Equal(want, v) {
		t.Fatalf("unexpected bytes: want %q, have %q", want, v)
	}
	var b bytes.Buffer
	if err := f.SerializeTo(&b); err != nil {
		t.Fatalf("serialization failed: %s", err)
	}
	if v := b.Bytes(); !bytes.Equal(want, v) {
		t.Fatalf("unexpected serialized bytes: want %q, have %q", want, v)
	}
}

func TestDestSme(t *testing.T) {
	var want []byte
	want = append(want, byte(0x01))        // flag
	want = append(want, byte(0x01))        // ton
	want = append(want, byte(0x01))        // npi
	want = append(want, []byte("1234")...) // Address
	want = append(want, byte(0x00))        // null terminator

	flag := Fixed{Data: byte(0x01)}
	ton := Fixed{Data: byte(0x01)}
	npi := Fixed{Data: byte(0x01)}
	destAddr := Variable{Data: []byte("1234")}
	fieldLen := flag.Len() + ton.Len() + npi.Len() + destAddr.Len()
	strRep := flag.String() + "," + ton.String() + "," + npi.String() + "," + destAddr.String()

	f := &DestSme{Flag: flag, Ton: ton, Npi: npi, DestAddr: destAddr}
	if f.Len() != fieldLen {
		t.Fatalf("unexpected len: want %d, have %d", fieldLen, f.Len())
	}
	if v, ok := f.Raw().([]byte); !ok {
		t.Fatalf("unexpected type: want []byte, have %#v", v)
	}
	if v := f.String(); v != string(strRep) {
		t.Fatalf("unexpected string: want %q have %q", strRep, v)
	}
	if v := f.Bytes(); !bytes.Equal(want, v) {
		t.Fatalf("unexpected bytes: want %q, have %q", want, v)
	}
	var b bytes.Buffer
	if err := f.SerializeTo(&b); err != nil {
		t.Fatalf("serialization failed: %s", err)
	}
	if v := b.Bytes(); !bytes.Equal(want, v) {
		t.Fatalf("unexpected serialized bytes: want %q, have %q", want, v)
	}
}

func TestDestSmeList(t *testing.T) {
	flag := Fixed{Data: byte(0x01)}
	ton := Fixed{Data: byte(0x01)}
	npi := Fixed{Data: byte(0x01)}
	destAddr := Variable{Data: []byte("1234")}
	destAddr2 := Variable{Data: []byte("5678")}

	sme1 := DestSme{Flag: flag, Ton: ton, Npi: npi, DestAddr: destAddr}
	sme2 := DestSme{Flag: flag, Ton: ton, Npi: npi, DestAddr: destAddr2}
	fieldLen := sme1.Len() + sme2.Len()
	strRep := sme1.String() + ";" + sme2.String() + ";"
	var bytesRep []byte
	bytesRep = append(bytesRep, sme1.Bytes()...)
	bytesRep = append(bytesRep, sme2.Bytes()...)

	f := &DestSmeList{Data: []DestSme{sme1, sme2}}
	if f.Len() != fieldLen {
		t.Fatalf("unexpected len: want %d, have %d", fieldLen, f.Len())
	}
	if v, ok := f.Raw().([]byte); !ok {
		t.Fatalf("unexpected type: want []byte, have %#v", v)
	}
	if v := f.String(); v != string(strRep) {
		t.Fatalf("unexpected string: want %q have %q", strRep, v)
	}
	if v := f.Bytes(); !bytes.Equal(bytesRep, v) {
		t.Fatalf("unexpected bytes: want %q, have %q", bytesRep, v)
	}
	var b bytes.Buffer
	if err := f.SerializeTo(&b); err != nil {
		t.Fatalf("serialization failed: %s", err)
	}
	if v := b.Bytes(); !bytes.Equal(bytesRep, v) {
		t.Fatalf("unexpected serialized bytes: want %q, have %q", bytesRep, v)
	}
}

func TestUnSme(t *testing.T) {
	errCode := [4]byte{0x00, 0x00, 0x00, 0x11}
	var want []byte
	want = append(want, byte(0x01))       // TON
	want = append(want, byte(0x01))       // NPI
	want = append(want, []byte("123")...) // Address
	want = append(want, byte(0x00))       // null terminator
	want = append(want, errCode[:]...)    // Error (4 bytes BE)

	ton := Fixed{Data: byte(0x01)}
	npi := Fixed{Data: byte(0x01)}
	destAddr := Variable{Data: []byte("123")}
	fieldLen := ton.Len() + npi.Len() + destAddr.Len() + len(errCode)
	strRep := ton.String() + "," + npi.String() + "," + destAddr.String() + "," + strconv.Itoa(17) // convertion to uint

	f := UnSme{Ton: ton, Npi: npi, DestAddr: destAddr, ErrCode: errCode}
	if f.Len() != fieldLen {
		t.Fatalf("unexpected len: want %d, have %d", fieldLen, f.Len())
	}
	if v, ok := f.Raw().([]byte); !ok {
		t.Fatalf("unexpected type: want []byte, have %#v", v)
	}
	if v := f.String(); v != string(strRep) {
		t.Fatalf("unexpected string: want %q have %q", strRep, v)
	}
	if v := f.Bytes(); !bytes.Equal(want, v) {
		t.Fatalf("unexpected bytes: want %q, have %q", want, v)
	}
	var b bytes.Buffer
	if err := f.SerializeTo(&b); err != nil {
		t.Fatalf("serialization failed: %s", err)
	}
	if v := b.Bytes(); !bytes.Equal(want, v) {
		t.Fatalf("unexpected serialized bytes: want %q, have %q", want, v)
	}
}

func TestUnSmeList(t *testing.T) {
	ton := Fixed{Data: byte(0x01)}
	npi := Fixed{Data: byte(0x01)}
	destAddr := Variable{Data: []byte("123")}
	destAddr2 := Variable{Data: []byte("456")}
	errCode := [4]byte{0x00, 0x00, 0x00, 0x11}

	unSme1 := UnSme{Ton: ton, Npi: npi, DestAddr: destAddr, ErrCode: errCode}
	unSme2 := UnSme{Ton: ton, Npi: npi, DestAddr: destAddr2, ErrCode: errCode}
	fieldLen := unSme1.Len() + unSme2.Len()
	strRep := unSme1.String() + ";" + unSme2.String() + ";"
	var bytesRep []byte
	bytesRep = append(bytesRep, unSme1.Bytes()...)
	bytesRep = append(bytesRep, unSme2.Bytes()...)

	f := &UnSmeList{Data: []UnSme{unSme1, unSme2}}
	if f.Len() != fieldLen {
		t.Fatalf("unexpected len: want %d, have %d", fieldLen, f.Len())
	}
	if v, ok := f.Raw().([]byte); !ok {
		t.Fatalf("unexpected type: want []byte, have %#v", v)
	}
	if v := f.String(); v != string(strRep) {
		t.Fatalf("unexpected string: want %q have %q", strRep, v)
	}
	if v := f.Bytes(); !bytes.Equal(bytesRep, v) {
		t.Fatalf("unexpected bytes: want %q, have %q", bytesRep, v)
	}
	var b bytes.Buffer
	if err := f.SerializeTo(&b); err != nil {
		t.Fatalf("serialization failed: %s", err)
	}
	if v := b.Bytes(); !bytes.Equal(bytesRep, v) {
		t.Fatalf("unexpected serialized bytes: want %q, have %q", bytesRep, v)
	}
}
