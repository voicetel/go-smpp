// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package smpp

import (
	"strings"
	"testing"
	"time"

	"github.com/voicetel/go-smpp/smpp/pdu"
	"github.com/voicetel/go-smpp/smpp/pdu/pdufield"
	"github.com/voicetel/go-smpp/smpp/pdu/pdutext"
	"github.com/voicetel/go-smpp/smpp/smpptest"
)

// query_sm_resp carries message_state and error_code as single integer
// octets (SMPP 3.4 §4.8.2); a nonzero error_code must not corrupt the
// decode (it previously ran the C-octet-string decoder past both bytes).
func TestQuerySM_NonzeroErrorCode(t *testing.T) {
	s := smpptest.NewUnstartedServer()
	s.Handler = func(c smpptest.Conn, p pdu.Body) {
		r := pdu.NewQuerySMResp()
		r.Header().Seq = p.Header().Seq
		r.Fields().Set(pdufield.MessageID, p.Fields()[pdufield.MessageID])
		r.Fields().Set(pdufield.MessageState, 5) // UNDELIVERABLE
		r.Fields().Set(pdufield.ErrorCode, 9)    // nonzero
		c.Write(r)
	}
	s.Start()
	defer s.Close()
	tx := &Transmitter{Addr: s.Addr(), User: smpptest.DefaultUser, Passwd: smpptest.DefaultPasswd}
	defer tx.Close()
	if conn := <-tx.Bind(); conn.Status() != Connected {
		t.Fatal(conn.Error())
	}
	qr, err := tx.QuerySM("root", "77", 5, 0)
	if err != nil {
		t.Fatal(err)
	}
	if qr.MsgState != "UNDELIVERABLE" {
		t.Fatalf("state: want UNDELIVERABLE, have %q", qr.MsgState)
	}
	if qr.ErrCode != 9 {
		t.Fatalf("err code: want 9, have %d", qr.ErrCode)
	}
}

// A single-PDU Submit whose encoded body exceeds the sm_length octet must
// be rejected rather than silently truncated modulo 256 on the wire.
func TestSubmit_ShortMessageTooLong(t *testing.T) {
	s := smpptest.NewUnstartedServer()
	s.Handler = func(c smpptest.Conn, p pdu.Body) {
		if p.Header().ID == pdu.SubmitSMID {
			r := pdu.NewSubmitSMResp()
			r.Header().Seq = p.Header().Seq
			r.Fields().Set(pdufield.MessageID, "ok")
			c.Write(r)
			return
		}
		smpptest.EchoHandler(c, p)
	}
	s.Start()
	defer s.Close()
	tx := &Transmitter{Addr: s.Addr(), User: smpptest.DefaultUser, Passwd: smpptest.DefaultPasswd}
	defer tx.Close()
	if conn := <-tx.Bind(); conn.Status() != Connected {
		t.Fatal(conn.Error())
	}
	_, err := tx.Submit(&ShortMessage{
		Src:  "a",
		Dst:  "b",
		Text: pdutext.Raw(make([]byte, MaxShortMessageLen+1)),
	})
	if err != ErrShortMessageTooLong {
		t.Fatalf("want ErrShortMessageTooLong, have %v", err)
	}
	// Exactly at the limit is accepted (no truncation).
	if _, err := tx.Submit(&ShortMessage{
		Src:  "a",
		Dst:  "b",
		Text: pdutext.Raw(make([]byte, MaxShortMessageLen)),
	}); err != nil {
		t.Fatalf("at-limit submit rejected: %v", err)
	}
}

// convertValidity's relative form must stay a fixed 16-octet field for
// every duration, including >= 100 days (which overflowed the 2-digit DD
// field) and negative durations.
func TestConvertValidity_Relative(t *testing.T) {
	cases := []struct {
		name string
		d    time.Duration
	}{
		{"zero", 0},
		{"seconds", 45 * time.Second},
		{"one day", 24 * time.Hour},
		{"ninety-nine days", 99 * 24 * time.Hour},
		{"hundred days", 100 * 24 * time.Hour},
		{"one year plus", 400 * 24 * time.Hour},
		{"negative", -time.Hour},
	}
	for _, c := range cases {
		got := convertValidity(c.d, true)
		if len(got) != 16 {
			t.Fatalf("%s: relative validity %q is %d chars, want 16", c.name, got, len(got))
		}
		if !strings.HasSuffix(got, "000R") {
			t.Fatalf("%s: relative validity %q missing 000R suffix", c.name, got)
		}
	}
}

// pdufield.New must not panic on a caller-supplied truncated UDH (an IE
// whose declared length overruns the buffer, or a trailing partial IE).
func TestNewGSMUserData_TruncatedDoesNotPanic(t *testing.T) {
	for _, b := range [][]byte{
		{0x00, 0x03, 0x01},                   // IELength 3 but only 1 data byte follows
		{0x00, 0x05, 0x01, 0x02},             // IELength 5, 2 bytes follow
		{0x00},                               // IEI with no length byte
		{0x00, 0x03, 0x01, 0x02, 0x03, 0x08}, // valid IE then a trailing lone byte
	} {
		// New must return without panicking; content correctness is
		// covered elsewhere — this only guards the bounds.
		_ = pdufield.New(pdufield.GSMUserData, b)
	}
}
