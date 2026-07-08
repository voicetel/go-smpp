// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package smpp

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/voicetel/go-smpp/smpp/pdu"
	"github.com/voicetel/go-smpp/smpp/pdu/pdufield"
)

// Receiver implements an SMPP client receiver.
type Receiver struct {
	Addr                 string
	User                 string
	Passwd               string
	SystemType           string
	EnquireLink          time.Duration
	EnquireLinkTimeout   time.Duration // Time after last EnquireLink response when connection considered down
	BindInterval         time.Duration // Binding retry interval
	MergeInterval        time.Duration // Time in which Receiver waits for the parts of the long messages
	MergeCleanupInterval time.Duration // How often to cleanup expired message parts
	TLS                  *tls.Config
	Handler              HandlerFunc
	SkipAutoRespondIDs   []pdu.ID

	chanClose chan struct{}
	closeOnce sync.Once // guards close(chanClose) against a double Close()

	// struct which holds the map of MergeHolders for the merging of the long incoming messages.
	// It is used only if the incoming PDU holds UDH data and Receiver has MergeInterval > 0.
	mg struct {
		mergeHolders map[int]*MergeHolder
		sync.Mutex
	}

	cl struct {
		*client
		sync.Mutex
	}
}

// HandlerFunc is the handler function that a Receiver calls
// when a new PDU arrives.
type HandlerFunc func(p pdu.Body)

// MergeHolder is a struct which holds the slice of MessageParts for the merging of a long incoming message.
type MergeHolder struct {
	MessageID     int
	MessageParts  []*MessagePart // Slice with the parts of the message
	PartsCount    int
	LastWriteTime time.Time
}

// MessagePart is a struct which holds the data of the part of a long incoming message.
type MessagePart struct {
	PartID int
	Data   *bytes.Buffer
}

// Bind starts the Receiver. It creates a persistent connection
// to the server, update its status via the returned channel,
// and calls the registered Handler when new PDU arrives.
//
// Bind implements the ClientConn interface.
func (r *Receiver) Bind() <-chan ConnStatus {
	r.cl.Lock()
	defer r.cl.Unlock()

	if r.cl.client != nil {
		return r.cl.Status
	}

	// Create the close channel only on a genuine bind (past the
	// already-bound early return), so a redundant Bind() on a live
	// Receiver cannot overwrite the channel the running mergeCleaner is
	// selecting on. Reset closeOnce so this new session can be closed.
	r.chanClose = make(chan struct{})
	r.closeOnce = sync.Once{}

	c := &client{
		Addr:               r.Addr,
		TLS:                r.TLS,
		EnquireLink:        r.EnquireLink,
		EnquireLinkTimeout: r.EnquireLinkTimeout,
		Status:             make(chan ConnStatus, 1),
		BindFunc:           r.bindFunc,
		BindInterval:       r.BindInterval,
	}
	r.cl.client = c

	c.init()

	// Set up message merging before starting the connect goroutine, so
	// bindFunc's clear-on-rebind and the cleanup goroutine never race
	// with this initial map allocation.
	if r.MergeInterval > 0 {
		if r.MergeCleanupInterval == 0 {
			r.MergeCleanupInterval = 1 * time.Second
		}
		r.mg.Lock()
		r.mg.mergeHolders = make(map[int]*MergeHolder)
		r.mg.Unlock()
		go r.mergeCleaner()
	}

	go c.Bind()

	return c.Status
}

func (r *Receiver) bindFunc(c Conn) error {
	p := pdu.NewBindReceiver()
	f := p.Fields()
	f.Set(pdufield.SystemID, r.User)
	f.Set(pdufield.Password, r.Passwd)
	f.Set(pdufield.SystemType, r.SystemType)
	resp, err := bind(c, p)
	if err != nil {
		return err
	}
	if resp.Header().ID != pdu.BindReceiverRespID {
		return fmt.Errorf("unexpected response for BindReceiver: %s",
			resp.Header().ID)
	}

	// Clean the map in case of rebind, because message id numbering resets after reconnection
	// and older IDs are no longer valid
	if r.MergeInterval > 0 {
		r.mg.Lock()
		r.mg.mergeHolders = make(map[int]*MergeHolder)
		r.mg.Unlock()
	}

	// Always start handlePDU so the inbox channel is drained even when
	// the user has not configured a Handler. Otherwise the unbuffered
	// inbox blocks the client read loop on the first inbound PDU.
	go r.handlePDU()

	return nil
}

func idInList(id pdu.ID, list []pdu.ID) bool {
	for _, x := range list {
		if x == id {
			return true
		}
	}
	return false
}

func (r *Receiver) handlePDU() {
	autoRespondDeliver := !idInList(pdu.DeliverSMID, r.SkipAutoRespondIDs)
	autoRespondData := !idInList(pdu.DataSMID, r.SkipAutoRespondIDs)

	for {
		p, err := r.cl.Read()
		if err != nil || p == nil {
			break
		}

		switch p.Header().ID {
		case pdu.DeliverSMID:
			if autoRespondDeliver { // Send DeliverSMResp
				pResp := pdu.NewDeliverSMRespSeq(p.Header().Seq)
				r.cl.Write(pResp)
			}
		case pdu.DataSMID:
			if autoRespondData { // Send DataSMResp (SMPP 3.4 §4.7.2)
				pResp := pdu.NewDataSMRespSeq(p.Header().Seq)
				r.cl.Write(pResp)
			}
		}

		if r.MergeInterval == 0 || !hasUDHI(p) {
			r.deliver(p)
			continue
		}

		sm, ok := p.Fields()[pdufield.ShortMessage].(*pdufield.SM)
		if !ok {
			continue
		}

		merged, ready := r.tryMerge(sm.Data)
		if !ready {
			continue
		}
		p.Fields().Set(pdufield.ShortMessage, merged)
		r.deliver(p)
	}
}

// deliver hands p to the user handler, if one is configured.
func (r *Receiver) deliver(p pdu.Body) {
	if r.Handler != nil {
		r.Handler(p)
	}
}

// hasUDHI reports whether the PDU has the UDHI bit set in esm_class,
// indicating that short_message is prefixed with a User Data Header.
func hasUDHI(p pdu.Body) bool {
	esm, ok := p.Fields()[pdufield.ESMClass].(*pdufield.Fixed)
	if !ok {
		return false
	}
	return esm.Data&0x40 != 0
}

// tryMerge accumulates a single concatenated-SMS part and returns the
// reassembled payload once every part has arrived. It supports both the
// 8-bit (IEI 0x00) and 16-bit (IEI 0x08) reference forms of UDH per
// 3GPP TS 23.040 §9.2.3.24. body is the entire short_message field,
// including the leading UDH length byte.
func (r *Receiver) tryMerge(body []byte) ([]byte, bool) {
	msgID, partsCount, partID, payload, ok := parseConcatUDH(body)
	if !ok {
		return nil, false
	}

	r.mg.Lock()
	mh, exists := r.mg.mergeHolders[msgID]
	if !exists {
		mh = &MergeHolder{MessageID: msgID, PartsCount: partsCount}
		r.mg.mergeHolders[msgID] = mh
	}
	mh.MessageParts = append(mh.MessageParts, &MessagePart{
		PartID: partID,
		Data:   bytes.NewBuffer(payload),
	})
	mh.LastWriteTime = time.Now()
	if len(mh.MessageParts) != mh.PartsCount {
		r.mg.Unlock()
		return nil, false
	}
	parts := mh.MessageParts
	delete(r.mg.mergeHolders, msgID)
	r.mg.Unlock()

	ordered := make([]*bytes.Buffer, partsCount)
	for _, mp := range parts {
		if mp.PartID < 1 || mp.PartID > partsCount {
			return nil, false
		}
		ordered[mp.PartID-1] = mp.Data
	}
	var buf bytes.Buffer
	for _, b := range ordered {
		if b == nil {
			return nil, false
		}
		buf.Write(b.Bytes())
	}
	return buf.Bytes(), true
}

// parseConcatUDH walks the IE list at the head of body looking for a
// concatenation IE (8- or 16-bit reference). The returned payload is
// body with the entire UDH (length byte + IEs) stripped.
func parseConcatUDH(body []byte) (msgID, partsCount, partID int, payload []byte, ok bool) {
	if len(body) < 1 {
		return 0, 0, 0, nil, false
	}
	udhLen := int(body[0])
	if udhLen < 5 || 1+udhLen > len(body) {
		return 0, 0, 0, nil, false
	}
	head := body[1 : 1+udhLen]
	payload = body[1+udhLen:]
	for i := 0; i+2 <= len(head); {
		iei := head[i]
		ielen := int(head[i+1])
		if i+2+ielen > len(head) {
			return 0, 0, 0, nil, false
		}
		ie := head[i+2 : i+2+ielen]
		switch iei {
		case 0x00: // concatenated SMS, 8-bit reference
			if ielen == 3 {
				return int(ie[0]), int(ie[1]), int(ie[2]), payload, true
			}
		case 0x08: // concatenated SMS, 16-bit reference
			if ielen == 4 {
				return int(ie[0])<<8 | int(ie[1]), int(ie[2]), int(ie[3]), payload, true
			}
		}
		i += 2 + ielen
	}
	return 0, 0, 0, nil, false
}

// mergeHoldersLen returns the number of in-flight partial messages.
// Used by tests to verify cleanup and successful-merge eviction.
func (r *Receiver) mergeHoldersLen() int {
	r.mg.Lock()
	defer r.mg.Unlock()
	return len(r.mg.mergeHolders)
}

func (r *Receiver) mergeCleaner() {
	ticker := time.NewTicker(r.MergeCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.mg.Lock()
			for _, mHolder := range r.mg.mergeHolders {
				if time.Since(mHolder.LastWriteTime) > r.MergeInterval {
					delete(r.mg.mergeHolders, mHolder.MessageID)
				}
			}
			r.mg.Unlock()

		case <-r.chanClose:
			return
		}
	}
}

// Close implements the ClientConn interface.
func (r *Receiver) Close() error {
	r.cl.Lock()
	defer r.cl.Unlock()
	if r.cl.client == nil {
		return ErrNotConnected
	}
	// closeOnce guards against a panic when Close() is called more than
	// once — natural since the embedded client.Close is itself idempotent
	// and Closer contracts usually tolerate repeat calls.
	r.closeOnce.Do(func() { close(r.chanClose) })
	return r.cl.Close()
}
