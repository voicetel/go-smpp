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
		if data != nil && len(data) > 2 {
			for i := 0; i < len(data); {
				udh := UDH{}
				udh.IEI = Fixed{Data: data[i]}
				udh.IELength = Fixed{Data: data[i+1]}
				l := int(data[i+1])
				udh.IEData = SM{Data: data[i+2 : i+2+l]}
				udhData = append(udhData, udh)
				i += l + 2
			}
		}
		return &UDHList{Data: udhData}
	default:
		return nil
	}
}
