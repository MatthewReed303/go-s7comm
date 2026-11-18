// SPDX-License-Identifier: MIT

package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeS7AnyTimersAndCountersByElement(t *testing.T) {
	for _, tc := range []struct {
		area byte
		want []byte
	}{
		{AreaTimer, []byte{0x12, 0x0A, 0x10, 0x1D, 0x00, 0x03, 0x00, 0x00, 0x1D, 0x00, 0x00, 0x05}},
		{AreaCounter, []byte{0x12, 0x0A, 0x10, 0x1C, 0x00, 0x03, 0x00, 0x00, 0x1C, 0x00, 0x00, 0x05}},
	} {
		got, err := EncodeS7Any(S7AnyAddress{Area: tc.area, Start: 5, Size: 3})
		if err != nil || !bytes.Equal(got, tc.want) {
			t.Errorf("area 0x%02X: % X, %v; want % X", tc.area, got, err, tc.want)
		}
	}
	if !IsElementArea(AreaTimer) || !IsElementArea(AreaCounter) || IsElementArea(AreaDB) {
		t.Error("IsElementArea")
	}
}

func TestEncodeWriteVarItemsRequest(t *testing.T) {
	req, err := EncodeWriteVarItemsRequest(7, []WriteVarItem{
		{Addr: S7AnyAddress{Area: AreaDB, DBNumber: 1, Start: 2, Size: 3}, Value: []byte{1, 2, 3}},
		{Addr: S7AnyAddress{Area: AreaCounter, Start: 1, Size: 1}, Value: []byte{0, 9}},
		{Addr: S7AnyAddress{Area: AreaMerkers, Start: 4, Size: 1}, Value: []byte{0xFF}},
	})
	if err != nil {
		t.Fatal(err)
	}
	h, rest, err := ParseS7Header(req)
	if err != nil || h.PDURef != 7 {
		t.Fatalf("header: %+v, %v", h, err)
	}
	param, data := rest[:h.ParamLength], rest[h.ParamLength:]
	specs, items, err := ParseWriteVarRequest(param, data)
	if err != nil || len(specs) != 3 || len(items) != 3 {
		t.Fatalf("ParseWriteVarRequest: %v (%d, %d)", err, len(specs), len(items))
	}
	if specs[1].TransportSize != AreaCounter || specs[1].StartBit != 1 || items[1].TransportSize != DataTransportSizeOctet || items[1].RawLength != 2 {
		t.Errorf("counter item: %+v %+v", specs[1], items[1])
	}
	if specs[0].StartBit != 16 || items[0].TransportSize != DataTransportSizeByte || items[0].RawLength != 24 || !bytes.Equal(items[0].Data, []byte{1, 2, 3}) {
		t.Errorf("DB item: %+v %+v", specs[0], items[0])
	}
	if !bytes.Equal(items[2].Data, []byte{0xFF}) || len(data) != 3*4+3+1+2+1 {
		t.Errorf("last item %+v, data %d bytes (the last one unpadded)", items[2], len(data))
	}
	if WriteVarItemSize(3) != 20 || WriteVarItemSize(2) != 18 {
		t.Error("WriteVarItemSize")
	}
}

func TestEncodeWriteVarItemsRequestLimits(t *testing.T) {
	ok := WriteVarItem{Addr: S7AnyAddress{Area: AreaDB, DBNumber: 1, Size: 1}, Value: []byte{1}}
	many := make([]WriteVarItem, MaxReadVarItems+1)
	for i := range many {
		many[i] = ok
	}
	for name, items := range map[string][]WriteVarItem{
		"none":      nil,
		"too many":  many,
		"too long":  {{Addr: ok.Addr, Value: make([]byte, MaxWriteVarBytes+1)}},
		"bad start": {{Addr: S7AnyAddress{Area: AreaDB, Start: -1}, Value: []byte{1}}},
	} {
		if _, err := EncodeWriteVarItemsRequest(1, items); !errors.Is(err, ErrOutOfRange) {
			t.Errorf("%s: %v, want ErrOutOfRange", name, err)
		}
	}
}
