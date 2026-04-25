// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package smpp

import (
	"testing"
	"time"

	"github.com/fiorix/go-smpp/v2/smpp/pdu"
	"github.com/fiorix/go-smpp/v2/smpp/pdu/pdufield"
	"github.com/fiorix/go-smpp/v2/smpp/smpptest"
)

// concat8Part builds a deliver_sm whose short_message body carries an
// 8-bit reference (IEI 0x00) concatenation UDH followed by text.
func concat8Part(refNum, total, seq byte, text string) pdu.Body {
	udh := []byte{0x05, 0x00, 0x03, refNum, total, seq}
	return deliverSMWithUDH(append(udh, []byte(text)...))
}

// concat16Part builds a deliver_sm whose short_message body carries a
// 16-bit reference (IEI 0x08) concatenation UDH followed by text.
func concat16Part(refHi, refLo, total, seq byte, text string) pdu.Body {
	udh := []byte{0x06, 0x08, 0x04, refHi, refLo, total, seq}
	return deliverSMWithUDH(append(udh, []byte(text)...))
}

func deliverSMWithUDH(body []byte) pdu.Body {
	p := pdu.NewDeliverSM()
	f := p.Fields()
	f.Set(pdufield.SourceAddr, "src")
	f.Set(pdufield.DestinationAddr, "dst")
	f.Set(pdufield.ESMClass, uint8(0x40)) // UDHI
	f.Set(pdufield.ShortMessage, body)
	return p
}

func newMergingReceiver(t *testing.T, mergeInterval, cleanupInterval time.Duration) (*smpptest.Server, *Receiver, chan pdu.Body) {
	t.Helper()
	s := smpptest.NewUnstartedServer()
	// Drop client-originated PDUs (otherwise EchoHandler would echo
	// the receiver's auto deliver_sm_resp back, polluting our channel).
	s.Handler = func(c smpptest.Conn, p pdu.Body) {}
	s.Start()
	rc := make(chan pdu.Body, 4)
	r := &Receiver{
		Addr:                 s.Addr(),
		User:                 smpptest.DefaultUser,
		Passwd:               smpptest.DefaultPasswd,
		MergeInterval:        mergeInterval,
		MergeCleanupInterval: cleanupInterval,
		Handler:              func(p pdu.Body) { rc <- p },
	}
	conn := <-r.Bind()
	if conn.Status() != Connected {
		t.Fatalf("bind: %v", conn.Error())
	}
	return s, r, rc
}

func waitForPDU(t *testing.T, rc <-chan pdu.Body) pdu.Body {
	t.Helper()
	select {
	case p := <-rc:
		return p
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for merged PDU")
	}
	return nil
}

func TestReceiverMergeUDH8Bit(t *testing.T) {
	s, r, rc := newMergingReceiver(t, time.Second, 100*time.Millisecond)
	defer r.Close()
	defer s.Close()

	s.BroadcastMessage(concat8Part(42, 2, 1, "hello "))
	s.BroadcastMessage(concat8Part(42, 2, 2, "world"))

	got := waitForPDU(t, rc)
	body := got.Fields()[pdufield.ShortMessage].Bytes()
	if string(body) != "hello world" {
		t.Fatalf("merged body: want %q, have %q", "hello world", string(body))
	}
	select {
	case extra := <-rc:
		t.Fatalf("unexpected extra PDU: %#v", extra)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReceiverMergeUDH16Bit(t *testing.T) {
	s, r, rc := newMergingReceiver(t, time.Second, 100*time.Millisecond)
	defer r.Close()
	defer s.Close()

	// Reference number = 0x1234 (4660).
	s.BroadcastMessage(concat16Part(0x12, 0x34, 2, 1, "foo "))
	s.BroadcastMessage(concat16Part(0x12, 0x34, 2, 2, "bar"))

	got := waitForPDU(t, rc)
	body := got.Fields()[pdufield.ShortMessage].Bytes()
	if string(body) != "foo bar" {
		t.Fatalf("merged body: want %q, have %q", "foo bar", string(body))
	}
}

func TestReceiverMergeOutOfOrder(t *testing.T) {
	s, r, rc := newMergingReceiver(t, time.Second, 100*time.Millisecond)
	defer r.Close()
	defer s.Close()

	s.BroadcastMessage(concat8Part(7, 2, 2, "B"))
	s.BroadcastMessage(concat8Part(7, 2, 1, "A"))

	got := waitForPDU(t, rc)
	body := got.Fields()[pdufield.ShortMessage].Bytes()
	if string(body) != "AB" {
		t.Fatalf("merged body: want %q, have %q", "AB", string(body))
	}
}

func TestReceiverMergeDeletesOnSuccess(t *testing.T) {
	s, r, rc := newMergingReceiver(t, time.Second, 100*time.Millisecond)
	defer r.Close()
	defer s.Close()

	s.BroadcastMessage(concat8Part(99, 2, 1, "x"))
	s.BroadcastMessage(concat8Part(99, 2, 2, "y"))
	waitForPDU(t, rc)

	if n := r.mergeHoldersLen(); n != 0 {
		t.Fatalf("merge map should be empty after success, have %d", n)
	}
}

func TestReceiverMergeCleanerExpires(t *testing.T) {
	s, r, _ := newMergingReceiver(t, 50*time.Millisecond, 20*time.Millisecond)
	defer r.Close()
	defer s.Close()

	s.BroadcastMessage(concat8Part(123, 2, 1, "alone"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.mergeHoldersLen() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("mergeCleaner did not evict expired entry; map size = %d", r.mergeHoldersLen())
}

func TestReceiverNonUDHIPDUNotMerged(t *testing.T) {
	s, r, rc := newMergingReceiver(t, time.Second, 100*time.Millisecond)
	defer r.Close()
	defer s.Close()

	p := pdu.NewDeliverSM()
	f := p.Fields()
	f.Set(pdufield.SourceAddr, "src")
	f.Set(pdufield.DestinationAddr, "dst")
	f.Set(pdufield.ShortMessage, []byte("plain"))
	s.BroadcastMessage(p)

	got := waitForPDU(t, rc)
	body := got.Fields()[pdufield.ShortMessage].Bytes()
	if string(body) != "plain" {
		t.Fatalf("non-UDHI body: want %q, have %q", "plain", string(body))
	}
}
