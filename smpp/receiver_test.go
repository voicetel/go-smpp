// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package smpp

import (
	"testing"
	"time"

	"github.com/voicetel/go-smpp/smpp/pdu"
	"github.com/voicetel/go-smpp/smpp/smpptest"
)

func TestReceiver(t *testing.T) {
	s := smpptest.NewServer()
	defer s.Close()
	rc := make(chan pdu.Body)
	r := &Receiver{
		Addr:    s.Addr(),
		User:    smpptest.DefaultUser,
		Passwd:  smpptest.DefaultPasswd,
		Handler: func(p pdu.Body) { rc <- p },
	}
	defer r.Close()
	conn := <-r.Bind()
	switch conn.Status() {
	case Connected:
	default:
		t.Fatal(conn.Error())
	}
	// trigger inbound message from server
	p := pdu.NewGenericNACK()
	s.BroadcastMessage(p)
	// check response.
	select {
	case m := <-rc:
		want, have := *p.Header(), *m.Header()
		if want != have {
			t.Fatalf("unexpected PDU: want %#v, have %#v",
				want, have)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server to echo")
	}
}

// TestReceiverNoHandlerDoesNotBlock guards against a regression where
// a Receiver bound without a Handler deadlocked the client read loop
// on the first inbound PDU because nothing drained the inbox channel.
func TestReceiverNoHandlerDoesNotBlock(t *testing.T) {
	s := smpptest.NewUnstartedServer()
	s.Handler = func(c smpptest.Conn, p pdu.Body) {} // drop client PDUs
	s.Start()
	defer s.Close()

	r := &Receiver{
		Addr:   s.Addr(),
		User:   smpptest.DefaultUser,
		Passwd: smpptest.DefaultPasswd,
		// Handler intentionally nil.
	}
	conn := <-r.Bind()
	if conn.Status() != Connected {
		t.Fatal(conn.Error())
	}

	for i := 0; i < 3; i++ {
		s.BroadcastMessage(pdu.NewDeliverSM())
	}

	done := make(chan struct{})
	go func() {
		r.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked: handler-less receiver deadlocked on inbox")
	}
}