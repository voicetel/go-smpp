// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pdu

import "testing"

// NewUnbindRespSeq must produce an unbind_resp PDU echoing the given sequence
// number, so a reply to a peer unbind carries the request's sequence.
func TestNewUnbindRespSeq(t *testing.T) {
	p := NewUnbindRespSeq(0x2A)
	if p.Header().ID != UnbindRespID {
		t.Fatalf("ID = %#x; want UnbindRespID %#x", p.Header().ID, UnbindRespID)
	}
	if p.Header().Seq != 0x2A {
		t.Fatalf("Seq = %#x; want 0x2A", p.Header().Seq)
	}
}
