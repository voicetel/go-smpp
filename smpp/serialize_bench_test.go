// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package smpp

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/voicetel/go-smpp/smpp/pdu"
	"github.com/voicetel/go-smpp/smpp/pdu/pdufield"
	"github.com/voicetel/go-smpp/smpp/pdu/pdutext"
)

// benchSubmitSM builds a representative submit_sm carrying the C-octet-string
// fields (service_type, source_addr, destination_addr, schedule_delivery_time,
// validity_period) that exercise Variable.SerializeTo, plus a short_message.
func benchSubmitSM() pdu.Body {
	p := pdu.NewSubmitSM(nil)
	f := p.Fields()
	f.Set(pdufield.ServiceType, "CMT")
	f.Set(pdufield.SourceAddr, "15551234567")
	f.Set(pdufield.DestinationAddr, "15559876543")
	f.Set(pdufield.ScheduleDeliveryTime, "")
	f.Set(pdufield.ValidityPeriod, "000024000000000R")
	f.Set(pdufield.ShortMessage, pdutext.GSM7("Hello, world! This is a benchmark message."))
	return p
}

// BenchmarkPDUSerializeTo measures serializing a submit_sm to a buffer —
// the Variable.SerializeTo allocation path (one per C-octet-string field).
func BenchmarkPDUSerializeTo(b *testing.B) {
	p := benchSubmitSM()
	var buf bytes.Buffer
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		if err := p.SerializeTo(&buf); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConnWrite measures conn.Write, which frames and flushes a PDU to
// the bufio writer over the socket.
func BenchmarkConnWrite(b *testing.B) {
	p := benchSubmitSM()
	c := &conn{w: bufio.NewWriter(io.Discard)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Write(p); err != nil {
			b.Fatal(err)
		}
	}
}

// benchDeliverSMWire builds the wire bytes of a representative deliver_sm
// MO message (source/dest addrs + GSM-7 short_message) for decode benchmarks.
func benchDeliverSMWire(b *testing.B) []byte {
	b.Helper()
	p := pdu.NewDeliverSM()
	f := p.Fields()
	f.Set(pdufield.ServiceType, "CMT")
	f.Set(pdufield.SourceAddr, "15551234567")
	f.Set(pdufield.DestinationAddr, "15559876543")
	f.Set(pdufield.ShortMessage, pdutext.GSM7("Reply YES to confirm your appointment on the 5th."))
	var buf bytes.Buffer
	if err := p.SerializeTo(&buf); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

// BenchmarkPDUDecode measures the inbound decode hot path: pdu.Decode of a
// deliver_sm wire buffer (header + field-list decode).
func BenchmarkPDUDecode(b *testing.B) {
	wire := benchDeliverSMWire(b)
	r := bytes.NewReader(wire)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Reset(wire)
		if _, err := pdu.Decode(r); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRoundTrip measures a full submit serialize + deliver decode cycle,
// approximating one outbound + one inbound PDU on a busy binding.
func BenchmarkRoundTrip(b *testing.B) {
	p := benchSubmitSM()
	wire := benchDeliverSMWire(b)
	var buf bytes.Buffer
	r := bytes.NewReader(wire)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		if err := p.SerializeTo(&buf); err != nil {
			b.Fatal(err)
		}
		r.Reset(wire)
		if _, err := pdu.Decode(r); err != nil {
			b.Fatal(err)
		}
	}
}
