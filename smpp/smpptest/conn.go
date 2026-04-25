// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package smpptest

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"sync"

	"github.com/voicetel/go-smpp/smpp/pdu"
)

// Conn implements a server side connection.
type Conn interface {
	// Write serializes the given PDU and writes to the connection.
	Write(p pdu.Body) error

	// Close terminates the connection.
	Close() error

	// RemoteAddr returns the peer address.
	RemoteAddr() net.Addr
}

// conn provides the basics of an SMPP connection. Writes are serialized
// by wmu so that concurrent callers (the accept-handler thread and any
// caller of Server.BroadcastMessage) cannot interleave on the buffered
// writer.
type conn struct {
	rwc net.Conn
	r   *bufio.Reader
	w   *bufio.Writer
	// wmu serializes writes: the server's handle goroutine (echoing via
	// srv.Handler) and test goroutines (Server.BroadcastMessage) share this
	// conn, and bufio.Writer is not safe for concurrent use.
	wmu sync.Mutex
}

func newConn(c net.Conn) *conn {
	return &conn{
		rwc: c,
		r:   bufio.NewReader(c),
		w:   bufio.NewWriter(c),
	}
}

// RemoteAddr implements the Conn interface.
func (c *conn) RemoteAddr() net.Addr {
	return c.rwc.RemoteAddr()
}

// Read reads PDU off the wire.
func (c *conn) Read() (pdu.Body, error) {
	return pdu.Decode(c.r)
}

// Write implements the Conn interface.
func (c *conn) Write(p pdu.Body) error {
	var b bytes.Buffer
	if err := p.SerializeTo(&b); err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if _, err := io.Copy(c.w, &b); err != nil {
		return err
	}
	return c.w.Flush()
}

// Close implements the Conn interface.
func (c *conn) Close() error {
	return c.rwc.Close()
}
