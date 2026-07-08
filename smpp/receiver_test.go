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

// TestClientAutoRespondsToUnbind asserts that an unbind sent by the
// SMSC is acknowledged with unbind_resp and the Unbind PDU is forwarded
// to the Handler for the caller to observe. The client deliberately does
// NOT proactively tear the connection down itself at this layer — that
// decision (and any Close()/reconnect) is left to the caller, so that a
// higher-level session-generation-aware close path (as the gateway
// implements) can't be raced by this client closing the connection out
// from under it. See the UnbindID case in client.go.
func TestClientAutoRespondsToUnbind(t *testing.T) {
	gotUnbindResp := make(chan struct{}, 1)
	s := smpptest.NewUnstartedServer()
	s.Handler = func(c smpptest.Conn, p pdu.Body) {
		if p.Header().ID == pdu.UnbindRespID {
			select {
			case gotUnbindResp <- struct{}{}:
			default:
			}
		}
	}
	s.Start()
	defer s.Close()

	rc := make(chan ConnStatus, 8)
	gotUnbind := make(chan struct{}, 1)
	r := &Receiver{
		Addr:   s.Addr(),
		User:   smpptest.DefaultUser,
		Passwd: smpptest.DefaultPasswd,
		Handler: func(p pdu.Body) {
			if p.Header().ID == pdu.UnbindID {
				select {
				case gotUnbind <- struct{}{}:
				default:
				}
			}
		},
	}
	defer r.Close()
	go func() {
		for c := range r.Bind() {
			rc <- c
		}
	}()

	// Wait for initial Connected before nudging.
	waitFor := func(want ConnStatusID) {
		t.Helper()
		deadline := time.After(2 * time.Second)
		for {
			select {
			case c := <-rc:
				if c.Status() == want {
					return
				}
			case <-deadline:
				t.Fatalf("timed out waiting for status %s", want)
			}
		}
	}
	waitFor(Connected)

	s.BroadcastMessage(pdu.NewUnbind())

	select {
	case <-gotUnbindResp:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive unbind_resp")
	}
	select {
	case <-gotUnbind:
	case <-time.After(2 * time.Second):
		t.Fatal("Handler did not receive the forwarded Unbind PDU")
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
