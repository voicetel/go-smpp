// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package smpp

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/voicetel/go-smpp/smpp/pdu"
	"github.com/voicetel/go-smpp/smpp/pdu/pdufield"
	"github.com/voicetel/go-smpp/smpp/pdu/pdutext"
	"github.com/voicetel/go-smpp/smpp/pdu/pdutlv"
)

// ErrMaxWindowSize is returned when an operation (such as Submit) violates
// the maximum window size configured for the Transmitter or Transceiver.
var ErrMaxWindowSize = errors.New("reached max window size")

// ErrShortMessageTooLong is returned when a single-PDU Submit is given a
// short_message whose encoded length exceeds MaxShortMessageLen. sm_length
// is a single octet (SMPP 3.4 §5.2.21); an over-length body would be
// silently truncated modulo 256 on the wire. Use SubmitLongMsg (UDH
// segmentation) or the message_payload TLV for longer content.
var ErrShortMessageTooLong = errors.New("short_message exceeds 254 octets; use SubmitLongMsg or message_payload")

// MaxShortMessageLen is the largest short_message the wire sm_length octet
// can represent (SMPP 3.4 §5.2.21: 0-254; 255 is reserved).
const MaxShortMessageLen = 254

// MaxDestinationAddress is the maximum number of destination addresses allowed
// in the submit_multi operation.
const MaxDestinationAddress = 254

// Transmitter implements an SMPP client transmitter.
type Transmitter struct {
	Addr               string        // Server address in form of host:port.
	User               string        // Username.
	Passwd             string        // Password.
	SystemType         string        // System type, default empty.
	EnquireLink        time.Duration // Enquire link interval, default 10s.
	EnquireLinkTimeout time.Duration // Time after last EnquireLink response when connection considered down
	RespTimeout        time.Duration // Response timeout, default 1s.
	BindInterval       time.Duration // Binding retry interval
	TLS                *tls.Config   // TLS client settings, optional.
	RateLimiter        RateLimiter   // Rate limiter, optional.
	WindowSize         uint
	// SkipAutoRespondIDs lists inbound PDU IDs (e.g. pdu.DeliverSMID,
	// pdu.DataSMID) the read loop should NOT auto-acknowledge, leaving
	// the response to the handler. Applies to Transmitter and, via
	// embedding, Transceiver — mirroring Receiver.SkipAutoRespondIDs so
	// a transceiver user can suppress auto-acks (and avoid double-acking
	// when responding manually).
	SkipAutoRespondIDs []pdu.ID
	rMutex             sync.Mutex
	r                  *rand.Rand

	cl struct {
		sync.Mutex
		*client
	}

	tx struct {
		count int32
		sync.Mutex
		// inflight maps a request's sequence number to the channel
		// waiting for its response. Keyed by bare seq (the client
		// assigns seq from a single monotonic counter, so outstanding
		// requests have distinct seqs); only response PDUs are matched
		// against it (see handlePDU), so a server-initiated request
		// reusing a seq from its own space can never collide.
		inflight map[uint32]chan *tx
	}
}

type tx struct {
	PDU pdu.Body
	Err error
}

// Bind implements the ClientConn interface.
//
// Any commands (e.g. Submit) attempted on a dead connection will
// return ErrNotConnected.
func (t *Transmitter) Bind() <-chan ConnStatus {
	t.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	t.cl.Lock()
	defer t.cl.Unlock()
	if t.cl.client != nil {
		return t.cl.Status
	}
	t.tx.Lock()
	t.tx.inflight = make(map[uint32]chan *tx)
	t.tx.Unlock()
	c := &client{
		Addr:               t.Addr,
		TLS:                t.TLS,
		Status:             make(chan ConnStatus, 1),
		BindFunc:           t.bindFunc,
		EnquireLink:        t.EnquireLink,
		EnquireLinkTimeout: t.EnquireLinkTimeout,
		RespTimeout:        t.RespTimeout,
		WindowSize:         t.WindowSize,
		RateLimiter:        t.RateLimiter,
		BindInterval:       t.BindInterval,
	}
	t.cl.client = c
	c.init()
	go c.Bind()
	return c.Status
}

func (t *Transmitter) bindFunc(c Conn) error {
	p := pdu.NewBindTransmitter()
	f := p.Fields()
	f.Set(pdufield.SystemID, t.User)
	f.Set(pdufield.Password, t.Passwd)
	f.Set(pdufield.SystemType, t.SystemType)
	resp, err := bind(c, p)
	if err != nil {
		return err
	}
	if resp.Header().ID != pdu.BindTransmitterRespID {
		return fmt.Errorf("unexpected response for BindTransmitter: %s",
			resp.Header().ID)
	}
	go t.handlePDU(nil)
	return nil
}

// f is only set on transceiver.
func (t *Transmitter) handlePDU(f HandlerFunc) {
	autoRespondDeliver := !idInList(pdu.DeliverSMID, t.SkipAutoRespondIDs)
	autoRespondData := !idInList(pdu.DataSMID, t.SkipAutoRespondIDs)
	for {
		p, err := t.cl.Read()
		if err != nil || p == nil {
			break
		}
		// Correlate only responses (response bit set) to the waiting
		// caller, matched by sequence number. A server-initiated
		// request (deliver_sm, data_sm, …) is never looked up — it goes
		// to the handler and is auto-acked below — so it cannot be
		// misrouted into an in-flight request's channel even when its
		// (independent) seq space collides with ours. generic_nack
		// (group 0x0000) is a response and matches its request by seq,
		// which a group-based key could not do.
		var rc chan *tx
		if p.Header().ID.IsResponse() {
			t.tx.Lock()
			rc = t.tx.inflight[p.Header().Seq]
			t.tx.Unlock()
		}
		if rc != nil {
			rc <- &tx{PDU: p}
		} else if f != nil {
			f(p)
		}
		switch p.Header().ID {
		case pdu.DeliverSMID:
			if autoRespondDeliver { // Send DeliverSMResp
				t.cl.Write(pdu.NewDeliverSMRespSeq(p.Header().Seq))
			}
		case pdu.DataSMID:
			if autoRespondData { // Send DataSMResp (SMPP 3.4 §4.7.2)
				t.cl.Write(pdu.NewDataSMRespSeq(p.Header().Seq))
			}
		}
	}
	// Connection lost: notify every waiting caller. Use a non-blocking
	// send — rc is buffered (cap 1), but a response delivered just
	// before a caller's RespTimeout can leave the buffer already full
	// with the caller gone; a blocking send here would then wedge this
	// loop forever while holding t.tx.Lock, deadlocking every future
	// Submit. The caller either drains the buffered value or times out.
	t.tx.Lock()
	for _, rc := range t.tx.inflight {
		select {
		case rc <- &tx{Err: ErrNotConnected}:
		default:
		}
	}
	t.tx.Unlock()
}

// Close implements the ClientConn interface.
func (t *Transmitter) Close() error {
	t.cl.Lock()
	defer t.cl.Unlock()
	if t.cl.client == nil {
		return ErrNotConnected
	}
	return t.cl.Close()
}

// UnsucessDest contains information about unsuccessful delivery to an address
// when submit multi is used
type UnsucessDest struct {
	AddrTON uint8
	AddrNPI uint8
	Address string
	Error   pdu.Status
}

// newUnsucessDest returns a new UnsucessDest constructed from a UnSme struct
func newUnsucessDest(p pdufield.UnSme) UnsucessDest {
	unDest := UnsucessDest{}
	unDest.AddrTON, _ = p.Ton.Raw().(uint8) // if there is an error default value will be set
	unDest.AddrNPI, _ = p.Npi.Raw().(uint8)
	unDest.Address = string(p.DestAddr.Bytes())
	unDest.Error = pdu.Status(binary.BigEndian.Uint32(p.ErrCode[:]))
	return unDest
}

// ShortMessage configures a short message that can be submitted via
// the Transmitter. When returned from Submit, the ShortMessage
// provides Resp and RespID.
type ShortMessage struct {
	Src              string
	Dst              string
	DstList          []string // List of destination addreses for submit multi
	DLs              []string //List if destribution list for submit multi
	Text             pdutext.Codec
	Validity         time.Duration
	RelativeValidity bool // Use relative format (000000HHMMSS000R) instead of absolute.
	Register         pdufield.DeliverySetting

	// Other fields, normally optional.
	TLVFields            pdutlv.Fields
	ServiceType          string
	SourceAddrTON        uint8
	SourceAddrNPI        uint8
	DestAddrTON          uint8
	DestAddrNPI          uint8
	ESMClass             uint8
	ProtocolID           uint8
	PriorityFlag         uint8
	ScheduleDeliveryTime string
	ReplaceIfPresentFlag uint8
	SMDefaultMsgID       uint8
	NumberDests          uint8

	resp *smResp
}

type smResp struct {
	sync.Mutex
	p pdu.Body
}

func (sm *ShortMessage) initResp() {
	if sm.resp == nil {
		sm.resp = &smResp{}
	}
}

// Resp returns the response PDU, or nil if not set.
func (sm *ShortMessage) Resp() pdu.Body {
	if sm.resp == nil {
		return nil
	}
	sm.resp.Lock()
	defer sm.resp.Unlock()
	return sm.resp.p
}

// RespID is a shortcut to Resp().Fields()[pdufield.MessageID].
// Returns empty if the response PDU is not available, or does
// not contain the MessageID field.
func (sm *ShortMessage) RespID() string {
	if sm.resp == nil {
		return ""
	}
	sm.resp.Lock()
	defer sm.resp.Unlock()
	if sm.resp.p == nil {
		return ""
	}
	f := sm.resp.p.Fields()[pdufield.MessageID]
	if f == nil {
		return ""
	}
	return f.String()
}

// NumbUnsuccess is a shortcut to Resp().Fields()[pdufield.NoUnsuccess].
// Returns zero and an error if the response PDU is not available, or does
// not contain the NoUnsuccess field.
func (sm *ShortMessage) NumbUnsuccess() (int, error) {
	if sm.resp == nil {
		return 0, errors.New("Response PDU not available")
	}
	sm.resp.Lock()
	defer sm.resp.Unlock()
	if sm.resp.p == nil {
		return 0, errors.New("Response PDU not available")
	}
	f := sm.resp.p.Fields()[pdufield.NoUnsuccess]
	if f == nil {
		return 0, errors.New("Response PDU does not contain NoUnsuccess field")
	}
	i, err := strconv.Atoi(f.String())
	if err != nil {
		return 0, fmt.Errorf("Failed to convert PDU value to string, error: %s", err.Error())
	}
	return i, nil
}

// UnsuccessSmes returns a list with the SME address(es) or/and Distribution List names to
// which submission was unsuccessful and the respective errors, when submit multi is used.
// Returns nil and an error if the response PDU is not available, or does
// not contain the unsuccess_sme field.
func (sm *ShortMessage) UnsuccessSmes() ([]UnsucessDest, error) {
	if sm.resp == nil {
		return nil, errors.New("Response PDU not available")
	}
	sm.resp.Lock()
	defer sm.resp.Unlock()
	if sm.resp.p == nil {
		return nil, errors.New("Response PDU not available")
	}
	f := sm.resp.p.Fields()[pdufield.UnsuccessSme]
	if f == nil {
		return nil, errors.New("Response PDU does not contain UnsuccessSme field")
	}
	usl, ok := f.(*pdufield.UnSmeList)
	if ok {
		var udl []UnsucessDest
		for i := range usl.Data {
			udl = append(udl, newUnsucessDest(usl.Data[i]))
		}
		return udl, nil
	}
	return nil, errors.New("Cannot convert PDU field to UnSmeList")
}

func (t *Transmitter) do(p pdu.Body) (*tx, error) {
	t.cl.Lock()
	notbound := t.cl.client == nil
	t.cl.Unlock()
	if notbound {
		return nil, ErrNotBound
	}
	if t.cl.WindowSize > 0 {
		inflight := uint(atomic.AddInt32(&t.tx.count, 1))
		defer func(t *Transmitter) { atomic.AddInt32(&t.tx.count, -1) }(t)
		if inflight > t.cl.WindowSize {
			return nil, ErrMaxWindowSize
		}
	}
	rc := make(chan *tx, 1)
	seq := p.Header().Seq
	t.tx.Lock()
	t.tx.inflight[seq] = rc
	t.tx.Unlock()
	defer func() {
		t.tx.Lock()
		delete(t.tx.inflight, seq)
		t.tx.Unlock()
	}()
	err := t.cl.Write(p)
	if err != nil {
		return nil, err
	}
	select {
	case resp := <-rc:
		if resp.Err != nil {
			return nil, resp.Err
		}
		return resp, nil
	case <-t.cl.respTimeout():
		return nil, ErrTimeout
	}
}

// Submit sends a short message and returns and updates the given
// sm with the response status. It returns the same sm object.
func (t *Transmitter) Submit(sm *ShortMessage) (*ShortMessage, error) {
	if len(sm.DstList) > 0 || len(sm.DLs) > 0 {
		// if we have a single destination address add it to the list
		if sm.Dst != "" {
			sm.DstList = append(sm.DstList, sm.Dst)
		}
		p := pdu.NewSubmitMulti(sm.TLVFields)
		return t.submitMsgMulti(sm, p, uint8(sm.Text.Type()))
	}
	p := pdu.NewSubmitSM(sm.TLVFields)
	return t.submitMsg(sm, p, uint8(sm.Text.Type()))
}

// SubmitLongMsg sends a long message (more than 140 bytes)
// and returns and updates the given sm with the response status.
// It returns the same sm object.
func (t *Transmitter) SubmitLongMsg(sm *ShortMessage) ([]ShortMessage, error) {
	maxLen := 133 // 140-7 (UDH with 2 byte reference number)
	switch sm.Text.(type) {
	case pdutext.GSM7:
		maxLen = 152 // to avoid an escape character being split between payloads
		break
	case pdutext.GSM7Packed:
		maxLen = 132 // to avoid an escape character being split between payloads
		break
	case pdutext.UCS2:
		maxLen = 132 // to avoid a character being split between payloads
		break
	}
	rawMsg := sm.Text.Encode()
	countParts := int((len(rawMsg)-1)/maxLen) + 1

	parts := make([]ShortMessage, 0, countParts)

	t.rMutex.Lock()
	rn := uint16(t.r.Intn(0xFFFF))
	t.rMutex.Unlock()
	UDHHeader := make([]byte, 7)
	UDHHeader[0] = 0x06              // length of user data header
	UDHHeader[1] = 0x08              // information element identifier, CSMS 16 bit reference number
	UDHHeader[2] = 0x04              // length of remaining header
	UDHHeader[3] = uint8(rn >> 8)    // most significant byte of the reference number
	UDHHeader[4] = uint8(rn)         // least significant byte of the reference number
	UDHHeader[5] = uint8(countParts) // total number of message parts
	for i := 0; i < countParts; i++ {
		UDHHeader[6] = uint8(i + 1) // current message part
		p := pdu.NewSubmitSM(sm.TLVFields)
		f := p.Fields()
		f.Set(pdufield.SourceAddr, sm.Src)
		f.Set(pdufield.DestinationAddr, sm.Dst)
		if i != countParts-1 {
			f.Set(pdufield.ShortMessage, pdutext.Raw(append(UDHHeader, rawMsg[i*maxLen:(i+1)*maxLen]...)))
		} else {
			f.Set(pdufield.ShortMessage, pdutext.Raw(append(UDHHeader, rawMsg[i*maxLen:]...)))
		}
		f.Set(pdufield.RegisteredDelivery, uint8(sm.Register))
		if sm.Validity != time.Duration(0) {
			f.Set(pdufield.ValidityPeriod, convertValidity(sm.Validity, sm.RelativeValidity))
		}
		f.Set(pdufield.ServiceType, sm.ServiceType)
		f.Set(pdufield.SourceAddrTON, sm.SourceAddrTON)
		f.Set(pdufield.SourceAddrNPI, sm.SourceAddrNPI)
		f.Set(pdufield.DestAddrTON, sm.DestAddrTON)
		f.Set(pdufield.DestAddrNPI, sm.DestAddrNPI)
		f.Set(pdufield.ESMClass, 0x40)
		f.Set(pdufield.ProtocolID, sm.ProtocolID)
		f.Set(pdufield.PriorityFlag, sm.PriorityFlag)
		f.Set(pdufield.ScheduleDeliveryTime, sm.ScheduleDeliveryTime)
		f.Set(pdufield.ReplaceIfPresentFlag, sm.ReplaceIfPresentFlag)
		f.Set(pdufield.SMDefaultMsgID, sm.SMDefaultMsgID)
		f.Set(pdufield.DataCoding, uint8(sm.Text.Type()))
		resp, err := t.do(p)
		if err != nil {
			return nil, err
		}
		// Attach the response to a per-part copy with its own smResp, and
		// never touch the shared input sm's resp field: the old code did
		// `sm.resp = &smResp{...}` on every iteration, an unsynchronized
		// pointer swap that races any goroutine polling the original sm's
		// Resp()/RespID() while later parts are still submitting. Each
		// returned part keeps its own distinct response.
		part := *sm
		part.resp = &smResp{p: resp.PDU}
		if resp.PDU == nil {
			return parts, fmt.Errorf("unexpected empty PDU")
		}
		if id := resp.PDU.Header().ID; id != pdu.SubmitSMRespID {
			return parts, fmt.Errorf("unexpected PDU ID: %s", id)
		}
		if s := resp.PDU.Header().Status; s != 0 {
			return parts, s
		}
		if resp.Err != nil {
			return parts, resp.Err
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func (t *Transmitter) submitMsg(sm *ShortMessage, p pdu.Body, dataCoding uint8) (*ShortMessage, error) {
	if sm.Text != nil && len(sm.Text.Encode()) > MaxShortMessageLen {
		return nil, ErrShortMessageTooLong
	}
	f := p.Fields()
	f.Set(pdufield.SourceAddr, sm.Src)
	f.Set(pdufield.DestinationAddr, sm.Dst)
	f.Set(pdufield.ShortMessage, sm.Text)
	f.Set(pdufield.RegisteredDelivery, uint8(sm.Register))
	// Check if the message has validity set.
	if sm.Validity != time.Duration(0) {
		f.Set(pdufield.ValidityPeriod, convertValidity(sm.Validity, sm.RelativeValidity))
	}
	f.Set(pdufield.ServiceType, sm.ServiceType)
	f.Set(pdufield.SourceAddrTON, sm.SourceAddrTON)
	f.Set(pdufield.SourceAddrNPI, sm.SourceAddrNPI)
	f.Set(pdufield.DestAddrTON, sm.DestAddrTON)
	f.Set(pdufield.DestAddrNPI, sm.DestAddrNPI)
	f.Set(pdufield.ESMClass, sm.ESMClass)
	f.Set(pdufield.ProtocolID, sm.ProtocolID)
	f.Set(pdufield.PriorityFlag, sm.PriorityFlag)
	f.Set(pdufield.ScheduleDeliveryTime, sm.ScheduleDeliveryTime)
	f.Set(pdufield.ReplaceIfPresentFlag, sm.ReplaceIfPresentFlag)
	f.Set(pdufield.SMDefaultMsgID, sm.SMDefaultMsgID)
	f.Set(pdufield.DataCoding, dataCoding)
	resp, err := t.do(p)
	if err != nil {
		return nil, err
	}
	sm.initResp()
	sm.resp.Lock()
	sm.resp.p = resp.PDU
	sm.resp.Unlock()
	if resp.PDU == nil {
		return nil, fmt.Errorf("unexpected empty PDU")
	}
	if id := resp.PDU.Header().ID; id != pdu.SubmitSMRespID {
		return sm, fmt.Errorf("unexpected PDU ID: %s", id)
	}
	if s := resp.PDU.Header().Status; s != 0 {
		return sm, s
	}
	return sm, resp.Err
}

func (t *Transmitter) submitMsgMulti(sm *ShortMessage, p pdu.Body, dataCoding uint8) (*ShortMessage, error) {
	if sm.Text != nil && len(sm.Text.Encode()) > MaxShortMessageLen {
		return nil, ErrShortMessageTooLong
	}
	numberOfDest := len(sm.DstList) + len(sm.DLs) // TODO: Validate numbers and lists according to size
	if numberOfDest > MaxDestinationAddress {
		return nil, fmt.Errorf("Error: Max number of destination addresses allowed is %d, trying to send to %d",
			MaxDestinationAddress, numberOfDest)
	}
	// Put destination addresses and lists inside an byte array
	var bArray []byte
	// destination addresses
	for _, destAddr := range sm.DstList {
		// 1 - SME Address
		bArray = append(bArray, byte(0x01))
		bArray = append(bArray, byte(sm.DestAddrTON))
		bArray = append(bArray, byte(sm.DestAddrNPI))
		bArray = append(bArray, []byte(destAddr)...)
		// null terminator
		bArray = append(bArray, byte(0x00))
	}

	// distribution lists
	for _, destList := range sm.DLs {
		// 2 - Distribution List
		bArray = append(bArray, byte(0x02))
		bArray = append(bArray, []byte(destList)...)
		// null terminator
		bArray = append(bArray, byte(0x00))
	}

	f := p.Fields()
	f.Set(pdufield.SourceAddr, sm.Src)
	f.Set(pdufield.DestinationList, bArray)
	f.Set(pdufield.ShortMessage, sm.Text)
	f.Set(pdufield.NumberDests, uint8(numberOfDest))
	f.Set(pdufield.RegisteredDelivery, uint8(sm.Register))
	// Check if the message has validity set.
	if sm.Validity != time.Duration(0) {
		f.Set(pdufield.ValidityPeriod, convertValidity(sm.Validity, sm.RelativeValidity))
	}
	f.Set(pdufield.ServiceType, sm.ServiceType)
	f.Set(pdufield.SourceAddrTON, sm.SourceAddrTON)
	f.Set(pdufield.SourceAddrNPI, sm.SourceAddrNPI)
	f.Set(pdufield.ESMClass, sm.ESMClass)
	f.Set(pdufield.ProtocolID, sm.ProtocolID)
	f.Set(pdufield.PriorityFlag, sm.PriorityFlag)
	f.Set(pdufield.ScheduleDeliveryTime, sm.ScheduleDeliveryTime)
	f.Set(pdufield.ReplaceIfPresentFlag, sm.ReplaceIfPresentFlag)
	f.Set(pdufield.SMDefaultMsgID, sm.SMDefaultMsgID)
	f.Set(pdufield.DataCoding, dataCoding)
	resp, err := t.do(p)
	if err != nil {
		return nil, err
	}
	sm.initResp()
	sm.resp.Lock()
	sm.resp.p = resp.PDU
	sm.resp.Unlock()
	if resp.PDU == nil {
		return nil, fmt.Errorf("unexpected empty PDU")
	}
	if id := resp.PDU.Header().ID; id != pdu.SubmitMultiRespID {
		return sm, fmt.Errorf("unexpected PDU ID: %s", id)
	}
	if s := resp.PDU.Header().Status; s != 0 {
		return sm, s
	}
	return sm, resp.Err
}

// QueryResp contains the parsed the response of a QuerySM request.
type QueryResp struct {
	MsgID     string
	MsgState  string
	FinalDate string
	ErrCode   uint8
}

// QuerySM queries the delivery status of a message. It requires the
// source address (sender) with TON and NPI and message ID.
func (t *Transmitter) QuerySM(src, msgid string, srcTON, srcNPI uint8) (*QueryResp, error) {
	p := pdu.NewQuerySM()
	f := p.Fields()
	f.Set(pdufield.SourceAddr, src)
	f.Set(pdufield.SourceAddrTON, srcTON)
	f.Set(pdufield.SourceAddrNPI, srcNPI)
	f.Set(pdufield.MessageID, msgid)

	resp, err := t.do(p)
	if err != nil {
		return nil, err
	}
	if id := resp.PDU.Header().ID; id != pdu.QuerySMRespID {
		return nil, fmt.Errorf("unexpected PDU ID: %s", id)
	}
	if s := resp.PDU.Header().Status; s != 0 {
		return nil, s
	}
	f = resp.PDU.Fields()
	ms := f[pdufield.MessageState]
	if ms == nil {
		return nil, fmt.Errorf("no state available")
	}
	qr := &QueryResp{MsgID: msgid}
	switch ms.Bytes()[0] {
	case 0:
		qr.MsgState = "SCHEDULED"
	case 1:
		qr.MsgState = "ENROUTE"
	case 2:
		qr.MsgState = "DELIVERED"
	case 3:
		qr.MsgState = "EXPIRED"
	case 4:
		qr.MsgState = "DELETED"
	case 5:
		qr.MsgState = "UNDELIVERABLE"
	case 6:
		qr.MsgState = "ACCEPTED"
	case 7:
		qr.MsgState = "UNKNOWN"
	case 8:
		qr.MsgState = "REJECTED"
	case 9:
		qr.MsgState = "SKIPPED"
	default:
		qr.MsgState = fmt.Sprintf("UNKNOWN (%d)", ms.Bytes()[0])
	}
	if fd := f[pdufield.FinalDate]; fd != nil {
		qr.FinalDate = fd.String()
	}
	if ec := f[pdufield.ErrorCode]; ec != nil {
		qr.ErrCode = ec.Bytes()[0]
	}
	return qr, nil
}

// CancelSM requests cancellation of a previously submitted message.
// At minimum, sm.Src and msgID must be set. Per SMPP 3.4 §4.9 the
// SMSC may also use sm.Dst (with TON/NPI) and sm.ServiceType to
// disambiguate; pass an empty/zero value when not needed. Returns nil
// on a successful response.
func (t *Transmitter) CancelSM(sm *ShortMessage, msgID string) error {
	p := pdu.NewCancelSM()
	f := p.Fields()
	f.Set(pdufield.ServiceType, sm.ServiceType)
	f.Set(pdufield.MessageID, msgID)
	f.Set(pdufield.SourceAddrTON, sm.SourceAddrTON)
	f.Set(pdufield.SourceAddrNPI, sm.SourceAddrNPI)
	f.Set(pdufield.SourceAddr, sm.Src)
	f.Set(pdufield.DestAddrTON, sm.DestAddrTON)
	f.Set(pdufield.DestAddrNPI, sm.DestAddrNPI)
	f.Set(pdufield.DestinationAddr, sm.Dst)
	resp, err := t.do(p)
	if err != nil {
		return err
	}
	if id := resp.PDU.Header().ID; id != pdu.CancelSMRespID {
		return fmt.Errorf("unexpected PDU ID: %s", id)
	}
	if s := resp.PDU.Header().Status; s != 0 {
		return s
	}
	return nil
}

// ReplaceSM replaces the body of a previously submitted message that
// has not yet been delivered. The replacement uses sm.Text,
// sm.ScheduleDeliveryTime, sm.Validity / sm.RelativeValidity,
// sm.Register and sm.SMDefaultMsgID. Source address fields must match
// the original submission. Returns nil on a successful response.
func (t *Transmitter) ReplaceSM(sm *ShortMessage, msgID string) error {
	p := pdu.NewReplaceSM()
	f := p.Fields()
	f.Set(pdufield.MessageID, msgID)
	f.Set(pdufield.SourceAddrTON, sm.SourceAddrTON)
	f.Set(pdufield.SourceAddrNPI, sm.SourceAddrNPI)
	f.Set(pdufield.SourceAddr, sm.Src)
	f.Set(pdufield.ScheduleDeliveryTime, sm.ScheduleDeliveryTime)
	if sm.Validity != time.Duration(0) {
		f.Set(pdufield.ValidityPeriod, convertValidity(sm.Validity, sm.RelativeValidity))
	} else {
		f.Set(pdufield.ValidityPeriod, "")
	}
	f.Set(pdufield.RegisteredDelivery, uint8(sm.Register))
	f.Set(pdufield.SMDefaultMsgID, sm.SMDefaultMsgID)
	if sm.Text != nil {
		f.Set(pdufield.ShortMessage, sm.Text)
	}
	resp, err := t.do(p)
	if err != nil {
		return err
	}
	if id := resp.PDU.Header().ID; id != pdu.ReplaceSMRespID {
		return fmt.Errorf("unexpected PDU ID: %s", id)
	}
	if s := resp.PDU.Header().Status; s != 0 {
		return s
	}
	return nil
}

// SubmitData sends a data_sm and updates sm with the response. Unlike
// submit_sm, data_sm carries no inline short_message; the body must be
// supplied via the message_payload TLV
// (sm.TLVFields[pdutlv.TagMessagePayload]) when needed. Useful for
// payloads larger than 254 octets without UDH segmentation, on SMSCs
// that support it.
func (t *Transmitter) SubmitData(sm *ShortMessage) (*ShortMessage, error) {
	p := pdu.NewDataSM(sm.TLVFields)
	f := p.Fields()
	f.Set(pdufield.ServiceType, sm.ServiceType)
	f.Set(pdufield.SourceAddrTON, sm.SourceAddrTON)
	f.Set(pdufield.SourceAddrNPI, sm.SourceAddrNPI)
	f.Set(pdufield.SourceAddr, sm.Src)
	f.Set(pdufield.DestAddrTON, sm.DestAddrTON)
	f.Set(pdufield.DestAddrNPI, sm.DestAddrNPI)
	f.Set(pdufield.DestinationAddr, sm.Dst)
	f.Set(pdufield.ESMClass, sm.ESMClass)
	f.Set(pdufield.RegisteredDelivery, uint8(sm.Register))
	if sm.Text != nil {
		f.Set(pdufield.DataCoding, uint8(sm.Text.Type()))
	}
	resp, err := t.do(p)
	if err != nil {
		return nil, err
	}
	sm.initResp()
	sm.resp.Lock()
	sm.resp.p = resp.PDU
	sm.resp.Unlock()
	if resp.PDU == nil {
		return nil, fmt.Errorf("unexpected empty PDU")
	}
	if id := resp.PDU.Header().ID; id != pdu.DataSMRespID {
		return sm, fmt.Errorf("unexpected PDU ID: %s", id)
	}
	if s := resp.PDU.Header().Status; s != 0 {
		return sm, s
	}
	return sm, resp.Err
}

func convertValidity(d time.Duration, relative bool) string {
	if relative {
		// Relative time format YYMMDDhhmmsstnnp, see SMPP 3.4 §7.1.1.
		// Every field is exactly two digits, so days must roll up into
		// (approximate) months and years rather than overflow the DD
		// field — a >=100-day validity otherwise produced a 17-char,
		// malformed period. Relative validity is inherently approximate;
		// use 30-day months and 365-day years. Negative durations clamp
		// to zero.
		total := int(d.Seconds())
		if total < 0 {
			total = 0
		}
		seconds := total % 60
		total /= 60
		minutes := total % 60
		total /= 60
		hours := total % 24
		total /= 24 // total is now whole days
		years := total / 365
		total %= 365
		months := total / 30
		days := total % 30
		if years > 99 {
			years = 99 // clamp at the two-digit field maximum
		}
		return fmt.Sprintf("%02d%02d%02d%02d%02d%02d000R", years, months, days, hours, minutes, seconds)
	}
	validity := time.Now().UTC().Add(d)
	// Absolute time format YYMMDDhhmmsstnnp, see SMPP 3.4 §7.1.1.
	return validity.Format("060102150405") + "000+"
}
