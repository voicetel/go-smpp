// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package smpp

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/voicetel/go-smpp/smpp/pdu"
)

var (
	// ErrNotConnected is returned on attempts to use a dead connection.
	ErrNotConnected = errors.New("not connected")

	// ErrNotBound is returned on attempts to use a Transmitter,
	// Receiver or Transceiver before calling Bind.
	ErrNotBound = errors.New("not bound")

	// ErrTimeout is returned when we've reached timeout while waiting for response.
	ErrTimeout = errors.New("timeout waiting for response")
)

// Conn is an SMPP connection.
type Conn interface {
	Reader
	Writer
	Closer
}

// Reader is the interface that wraps the basic Read method.
type Reader interface {
	// Read reads PDU binary data off the wire and returns it.
	Read() (pdu.Body, error)
}

// Writer is the interface that wraps the basic Write method.
type Writer interface {
	// Write serializes the given PDU and writes to the connection.
	Write(w pdu.Body) error
}

// Closer is the interface that wraps the basic Close method.
type Closer interface {
	// Close terminates the connection.
	Close() error
}

// Dial dials to the SMPP server and returns a Conn, or error.
// TLS is only used if provided.
func Dial(addr string, TLS *tls.Config) (Conn, error) {
	if addr == "" {
		addr = "localhost:2775"
	}
	fd, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if TLS != nil {
		fd = tls.Client(fd, TLS)
	}
	c := &conn{
		rwc: fd,
		r:   bufio.NewReader(fd),
		w:   bufio.NewWriter(fd),
	}
	return c, nil
}

// conn provides the basics of a single client connection and
// implements the Conn interface.
type conn struct {
	rwc net.Conn
	r   *bufio.Reader
	w   *bufio.Writer
}

// Read implements the Conn interface.
func (c *conn) Read() (pdu.Body, error) {
	return pdu.Decode(c.r)
}

// Write implements the Conn interface.
func (c *conn) Write(w pdu.Body) error {
	var b bytes.Buffer
	err := w.SerializeTo(&b)
	if err != nil {
		return err
	}
	_, err = io.Copy(c.w, &b)
	if err != nil {
		return err
	}
	return c.w.Flush()
}

// Close implements the Conn interface.
func (c *conn) Close() error {
	return c.rwc.Close()
}

// connSwitch implements the Conn interface but allows switching
// the actual Conn object it wraps.
//
// If no Conn is available, any attempt to Read/Write/Close
// returns ErrNotConnected.
//
// mu guards only the c pointer and is never held across I/O. wmu
// serializes concurrent Write callers (bufio.Writer is not
// concurrency-safe and PDU framing must not interleave). Keeping the
// blocking write off mu is deliberate: when the peer's TCP window is
// full a Write blocks in the kernel, and Close must still be able to
// take mu, drop the pointer, and close the fd — which is what unblocks
// the stuck write. Holding mu across the write instead would pin Close
// (and the enquire-link watchdog's teardown) until TCP retransmit
// timeout, potentially minutes.
type connSwitch struct {
	mu  sync.Mutex // guards c
	wmu sync.Mutex // serializes Write I/O
	c   Conn
}

// conn returns the current underlying Conn (or nil) under mu.
func (cs *connSwitch) conn() Conn {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.c
}

// Set sets the underlying Conn with the given one.
// If we hold a Conn already, it will be closed before switching over.
func (cs *connSwitch) Set(c Conn) {
	cs.mu.Lock()
	old := cs.c
	cs.c = c
	cs.mu.Unlock()
	if old != nil {
		old.Close()
	}
}

// Read implements the Conn interface.
func (cs *connSwitch) Read() (pdu.Body, error) {
	conn := cs.conn()
	if conn == nil {
		return nil, ErrNotConnected
	}
	return conn.Read()
}

// Write implements the Conn interface.
func (cs *connSwitch) Write(w pdu.Body) error {
	conn := cs.conn()
	if conn == nil {
		return ErrNotConnected
	}
	cs.wmu.Lock()
	defer cs.wmu.Unlock()
	return conn.Write(w)
}

// Close implements the Conn interface.
func (cs *connSwitch) Close() error {
	cs.mu.Lock()
	conn := cs.c
	cs.c = nil
	cs.mu.Unlock()
	if conn == nil {
		return ErrNotConnected
	}
	return conn.Close()
}
