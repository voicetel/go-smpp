// Copyright 2015 go-smpp authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pdufield

import (
	"errors"
	"testing"
	"time"
)

func TestParseDeliveryReceipt(t *testing.T) {
	cases := []struct {
		name string
		body string
		want DeliveryReceipt
	}{
		{
			name: "canonical",
			body: "id:0123456789 sub:001 dlvrd:001 submit date:2401020304 done date:2401020405 stat:DELIVRD err:000 Text:hello",
			want: DeliveryReceipt{
				ID:         "0123456789",
				Submitted:  1,
				Delivered:  1,
				SubmitDate: time.Date(2024, 1, 2, 3, 4, 0, 0, time.UTC),
				DoneDate:   time.Date(2024, 1, 2, 4, 5, 0, 0, time.UTC),
				State:      "DELIVRD",
				Err:        "000",
				Text:       "hello",
			},
		},
		{
			name: "expired no text",
			body: "id:abc sub:001 dlvrd:000 submit date:2403040506 done date:2403040707 stat:EXPIRED err:001",
			want: DeliveryReceipt{
				ID:         "abc",
				Submitted:  1,
				Delivered:  0,
				SubmitDate: time.Date(2024, 3, 4, 5, 6, 0, 0, time.UTC),
				DoneDate:   time.Date(2024, 3, 4, 7, 7, 0, 0, time.UTC),
				State:      "EXPIRED",
				Err:        "001",
			},
		},
		{
			name: "with seconds in date",
			body: "id:Z stat:DELIVRD submit date:240102030405 done date:240102040506",
			want: DeliveryReceipt{
				ID:         "Z",
				SubmitDate: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
				DoneDate:   time.Date(2024, 1, 2, 4, 5, 6, 0, time.UTC),
				State:      "DELIVRD",
			},
		},
		{
			name: "minimal",
			body: "id:42",
			want: DeliveryReceipt{ID: "42"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dr, err := ParseDeliveryReceipt([]byte(tc.body))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if *dr != tc.want {
				t.Fatalf("\n have %+v\n want %+v", *dr, tc.want)
			}
		})
	}
}

func TestParseDeliveryReceiptNotAReceipt(t *testing.T) {
	_, err := ParseDeliveryReceipt([]byte("just a regular MO message"))
	if !errors.Is(err, ErrNoReceipt) {
		t.Fatalf("want ErrNoReceipt, got %v", err)
	}
}

func TestParseDeliveryReceiptEmpty(t *testing.T) {
	_, err := ParseDeliveryReceipt(nil)
	if !errors.Is(err, ErrNoReceipt) {
		t.Fatalf("want ErrNoReceipt, got %v", err)
	}
}
