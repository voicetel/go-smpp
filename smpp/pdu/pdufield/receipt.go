// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pdufield

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// DeliveryReceipt is the parsed form of a deliver_sm body that carries
// a delivery receipt, per SMPP 3.4 Appendix B. The canonical wire form
// is:
//
//	id:NNNN sub:SSS dlvrd:DDD submit date:YYMMDDhhmm
//	  done date:YYMMDDhhmm stat:STATE err:EEE Text:...
//
// All fields except ID are optional; only ID is required for the
// parser to succeed. Date fields are interpreted as UTC.
type DeliveryReceipt struct {
	ID         string
	Submitted  int
	Delivered  int
	SubmitDate time.Time
	DoneDate   time.Time
	State      string
	Err        string
	Text       string
}

// ErrNoReceipt is returned by ParseDeliveryReceipt when the body does
// not contain a recognisable receipt (no "id:" token).
var ErrNoReceipt = errors.New("not a delivery receipt")

// dateLayout matches the YYMMDDhhmm form most SMSCs emit. A handful
// also append seconds; we accept that variant too.
const (
	receiptDate     = "0601021504"
	receiptDateSecs = "060102150405"
)

// ParseDeliveryReceipt scans body for the standard receipt key:value
// tokens and returns a populated DeliveryReceipt. Tokens may appear in
// any order; whitespace between tokens is tolerated. The Text field
// receives whatever follows the "Text:" marker verbatim, up to the end
// of body.
func ParseDeliveryReceipt(body []byte) (*DeliveryReceipt, error) {
	s := string(body)
	idx := strings.Index(strings.ToLower(s), "id:")
	if idx < 0 {
		return nil, ErrNoReceipt
	}
	dr := &DeliveryReceipt{}

	// Text is an open-ended trailing field; capture it once and trim
	// before tokenising.
	if i := strings.Index(s, "Text:"); i >= 0 {
		dr.Text = s[i+len("Text:"):]
		s = s[:i]
	} else if i := strings.Index(s, "text:"); i >= 0 {
		dr.Text = s[i+len("text:"):]
		s = s[:i]
	}

	tokens := tokeniseReceipt(s)
	for k, v := range tokens {
		switch strings.ToLower(k) {
		case "id":
			dr.ID = v
		case "sub":
			if n, err := strconv.Atoi(v); err == nil {
				dr.Submitted = n
			}
		case "dlvrd":
			if n, err := strconv.Atoi(v); err == nil {
				dr.Delivered = n
			}
		case "submit date":
			dr.SubmitDate = parseReceiptDate(v)
		case "done date":
			dr.DoneDate = parseReceiptDate(v)
		case "stat":
			dr.State = v
		case "err":
			dr.Err = v
		}
	}
	if dr.ID == "" {
		return nil, ErrNoReceipt
	}
	return dr, nil
}

// tokeniseReceipt splits on whitespace, then groups the two-word keys
// "submit date" and "done date" with their values.
func tokeniseReceipt(s string) map[string]string {
	out := make(map[string]string, 8)
	fields := strings.Fields(s)
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		// Two-word keys: "submit date:Y..." may appear as either
		// "submit" "date:Y..." or "submit" "date" "Y...". Only the
		// first form actually appears on the wire from SMSCs; handle
		// both for robustness.
		if (strings.EqualFold(f, "submit") || strings.EqualFold(f, "done")) && i+1 < len(fields) {
			next := fields[i+1]
			if strings.HasPrefix(strings.ToLower(next), "date:") {
				key := strings.ToLower(f) + " date"
				out[key] = next[len("date:"):]
				i++
				continue
			}
		}
		k, v, ok := strings.Cut(f, ":")
		if !ok {
			continue
		}
		out[k] = v
	}
	return out
}

func parseReceiptDate(v string) time.Time {
	for _, layout := range []string{receiptDate, receiptDateSecs} {
		if t, err := time.ParseInLocation(layout, v, time.UTC); err == nil {
			return t
		}
	}
	return time.Time{}
}
