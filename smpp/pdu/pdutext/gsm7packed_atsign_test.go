// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pdutext

import "testing"

// TestGSM7Packed_MidStreamAtSign pins that a '@' (GSM-7 code 0x00) landing at
// a 7-octet block boundary mid-message is NOT dropped by the unpacker's
// padding heuristic. Only the FINAL block's spare high bits can be zero
// padding (3GPP TS 23.038 §6.1.2.1.1); a non-final block's 8th septet is
// always a real character. The classic 7-septet trailing-'@' ambiguity
// (unresolvable without an explicit septet count) is deliberately NOT
// asserted here.
func TestGSM7Packed_MidStreamAtSign(t *testing.T) {
	cases := []string{
		"1234567@ABCDEFGH",  // '@' is septet 8 (first block boundary)
		"hello@world",       // '@' mid-message, not on a boundary (control)
		"Test@example.com",  // realistic email
		"12345678901234@Z",  // '@' at septet 15/16 area
		"AAAAAAA@BBBBBBB@C", // '@' at septets 8 and 16
	}
	for _, in := range cases {
		enc := GSM7Packed(in).Encode()
		got := string(GSM7Packed(enc).Decode())
		if got != in {
			t.Errorf("packed round-trip dropped/added a char: in=%q enc=%x got=%q", in, enc, got)
		}
	}
}
