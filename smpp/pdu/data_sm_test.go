// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pdu

import (
	"bytes"
	"testing"

	"github.com/voicetel/go-smpp/smpp/pdu/pdufield"
	"github.com/voicetel/go-smpp/smpp/pdu/pdutlv"
)

// data_sm must round-trip: serialize a DataSM with mandatory fields plus a
// message_payload TLV, decode it back, and recover the fields and TLV. Before
// this the codec returned "PDU not implemented" for data_sm.
func TestDataSM_RoundTrip(t *testing.T) {
	p := NewDataSM()
	f := p.Fields()
	f.Set(pdufield.SourceAddr, "12345")
	f.Set(pdufield.DestinationAddr, "67890")
	f.Set(pdufield.ESMClass, 0x04) // delivery receipt bit
	f.Set(pdufield.DataCoding, 0x00)
	if err := p.TLVFields().Set(pdutlv.TagMessagePayload, []byte("hello")); err != nil {
		t.Fatalf("set TLV: %v", err)
	}
	p.Header().Seq = 7

	var b bytes.Buffer
	if err := p.SerializeTo(&b); err != nil {
		t.Fatalf("serialize: %v", err)
	}

	got, err := Decode(&b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Header().ID != DataSMID {
		t.Fatalf("ID = %#x; want DataSMID", got.Header().ID)
	}
	if got.Header().Seq != 7 {
		t.Fatalf("Seq = %d; want 7", got.Header().Seq)
	}
	gf := got.Fields()
	if s := gf[pdufield.SourceAddr].String(); s != "12345" {
		t.Fatalf("source_addr = %q; want 12345", s)
	}
	if s := gf[pdufield.DestinationAddr].String(); s != "67890" {
		t.Fatalf("dest_addr = %q; want 67890", s)
	}
	mp, ok := got.TLVFields()[pdutlv.TagMessagePayload]
	if !ok || !bytes.Equal(mp.Bytes(), []byte("hello")) {
		t.Fatalf("message_payload TLV not recovered: %v", mp)
	}

	// data_sm_resp round-trips its message_id.
	r := NewDataSMResp()
	r.Fields().Set(pdufield.MessageID, "msg-1")
	r.Header().Seq = 7
	var rb bytes.Buffer
	if err := r.SerializeTo(&rb); err != nil {
		t.Fatalf("resp serialize: %v", err)
	}
	gr, err := Decode(&rb)
	if err != nil {
		t.Fatalf("resp decode: %v", err)
	}
	if gr.Header().ID != DataSMRespID {
		t.Fatalf("resp ID = %#x; want DataSMRespID", gr.Header().ID)
	}
	if id := gr.Fields()[pdufield.MessageID].String(); id != "msg-1" {
		t.Fatalf("resp message_id = %q; want msg-1", id)
	}
}
