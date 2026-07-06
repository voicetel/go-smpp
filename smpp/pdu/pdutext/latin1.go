// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pdutext

import (
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// Latin1 text codec.
type Latin1 []byte

// Type implements the Codec interface.
func (s Latin1) Type() DataCoding {
	return Latin1Type
}

// Encode to Latin1 (ISO/IEC 8859-1). data_coding 0x03 declares ISO-8859-1
// (IANA ISO_8859-1:1987, MIBenum 4, RFC 1345), whose 0x80-0x9F range is C1
// controls — NOT the Windows-1252 printable overlay. Use the exact ISO-8859-1
// charmap so bytes on the wire match the declared coding; callers must not pass
// text outside ISO-8859-1 (e.g. €, smart quotes) — that belongs in UCS2.
func (s Latin1) Encode() []byte {
	e := charmap.ISO8859_1.NewEncoder()
	es, _, err := transform.Bytes(e, s)
	if err != nil {
		return s
	}
	return es
}

// Decode from Latin1 (ISO/IEC 8859-1).
func (s Latin1) Decode() []byte {
	e := charmap.ISO8859_1.NewDecoder()
	es, _, err := transform.Bytes(e, s)
	if err != nil {
		return s
	}
	return es
}
