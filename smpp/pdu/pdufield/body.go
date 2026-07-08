// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pdufield

import "io"

// Body is an interface for manipulating binary PDU field data.
type Body interface {
	Len() int
	Raw() interface{}
	String() string
	Bytes() []byte
	SerializeTo(w io.Writer) error
}

// New parses the given binary data and returns a Data object,
// or nil if the field Name is unknown.
func New(n Name, data []byte) Body {
	switch n {
	case
		AddrNPI,
		AddrTON,
		DataCoding,
		DestAddrNPI,
		DestAddrTON,
		ESMAddrNPI,
		ESMAddrTON,
		ESMClass,
		ErrorCode,
		InterfaceVersion,
		MessageState,
		NumberDests,
		NoUnsuccess,
		PriorityFlag,
		ProtocolID,
		RegisteredDelivery,
		ReplaceIfPresentFlag,
		SMDefaultMsgID,
		SMLength,
		SourceAddrNPI,
		SourceAddrTON,
		UDHLength:
		if data == nil {
			data = []byte{0}
		}
		return &Fixed{Data: data[0]}
	case
		AddressRange,
		DestinationAddr,
		DestinationList,
		ESMAddr,
		FinalDate,
		MessageID,
		Password,
		ScheduleDeliveryTime,
		ServiceType,
		SourceAddr,
		SystemID,
		SystemType,
		UnsuccessSme,
		ValidityPeriod:
		if data == nil {
			data = []byte{}
		}
		return &Variable{Data: data}
	case ShortMessage:
		if data == nil {
			data = []byte{}
		}
		return &SM{Data: data}
	case GSMUserData:
		udhData := []UDH{}
		// Each IE is IEI(1) + IELength(1) + IEData(IELength). Bounds-check
		// every read: a truncated IE (IELength running past the buffer, or
		// a trailing byte with no length) must not panic on caller-supplied
		// bytes — the wire decoder in list.go is separately bounded.
		for i := 0; i+2 <= len(data); {
			l := int(data[i+1])
			if i+2+l > len(data) {
				break // declared IE length overruns the buffer
			}
			udhData = append(udhData, UDH{
				IEI:      Fixed{Data: data[i]},
				IELength: Fixed{Data: data[i+1]},
				IEData:   SM{Data: data[i+2 : i+2+l]},
			})
			i += l + 2
		}
		return &UDHList{Data: udhData}
	default:
		return nil
	}
}
