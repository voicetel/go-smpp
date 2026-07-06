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

// An SMSC-originated data_sm must be acknowledged with data_sm_resp echoing
// the request's sequence number (SMPP 3.4 §4.7.2) — mirroring the existing
// deliver_sm auto-response. Without it the SMSC retransmits and eventually
// stalls its window.
func TestReceiverDataSMAutoAck(t *testing.T) {
	ack := make(chan pdu.Body, 1)
	s := smpptest.NewUnstartedServer()
	s.Handler = func(c smpptest.Conn, p pdu.Body) {
		if p.Header().ID == pdu.DataSMRespID {
			select {
			case ack <- p:
			default:
			}
		}
	}
	s.Start()
	defer s.Close()

	rc := make(chan pdu.Body, 1)
	r := &Receiver{
		Addr:    s.Addr(),
		User:    smpptest.DefaultUser,
		Passwd:  smpptest.DefaultPasswd,
		Handler: func(p pdu.Body) { rc <- p },
	}
	defer r.Close()
	conn := <-r.Bind()
	if conn.Status() != Connected {
		t.Fatal(conn.Error())
	}

	ds := pdu.NewDataSM()
	seq := ds.Header().Seq
	s.BroadcastMessage(ds)

	select {
	case resp := <-ack:
		if resp.Header().Seq != seq {
			t.Fatalf("data_sm_resp seq = %d; want %d (echo of the request)", resp.Header().Seq, seq)
		}
		if resp.Header().Status != 0 {
			t.Fatalf("data_sm_resp status = %v; want 0 (ESME_ROK)", resp.Header().Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: no data_sm_resp received by server")
	}

	// The data_sm itself must still reach the application handler.
	select {
	case m := <-rc:
		if m.Header().ID != pdu.DataSMID {
			t.Fatalf("handler got %#x; want DataSMID", m.Header().ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: data_sm not forwarded to handler")
	}
}
