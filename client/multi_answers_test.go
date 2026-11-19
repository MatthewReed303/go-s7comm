// SPDX-License-Identifier: MIT

package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/otfabric/go-s7comm/model"
	"github.com/otfabric/go-s7comm/wire"
)

// multiPeer answers Read Var and Write Var requests of several items through
// read and write, and records the number of items of every request.
type multiPeer struct {
	read  func(ref uint16, specs []wire.VarSpec) []byte // nil reply: the connection ends
	write func(ref uint16, n int) []byte

	mu    sync.Mutex
	items []int
}

func (m *multiPeer) op(ref uint16, req []byte) []byte {
	plen := int(binary.BigEndian.Uint16(req[6:8]))
	dlen := int(binary.BigEndian.Uint16(req[8:10]))
	param := req[wire.S7HeaderSize : wire.S7HeaderSize+plen]
	data := req[wire.S7HeaderSize+plen : wire.S7HeaderSize+plen+dlen]
	switch param[0] {
	case wire.FuncReadVar:
		specs, err := wire.ParseReadVarRequest(param)
		if err != nil {
			return nil
		}
		m.record(len(specs))
		return m.read(ref, specs)
	case wire.FuncWriteVar:
		specs, _, err := wire.ParseWriteVarRequest(param, data)
		if err != nil {
			return nil
		}
		m.record(len(specs))
		return m.write(ref, len(specs))
	}
	return nil
}

func (m *multiPeer) record(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = append(m.items, n)
}

func (m *multiPeer) requests() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.items)
}

// multiClient connects a client to m over in-memory pipes (PDU size 480).
func multiClient(t *testing.T, m *multiPeer) *Client {
	t.Helper()
	peers := &pipePeers{op: m.op}
	t.Cleanup(peers.wait)
	return pipeClient(t, peers)
}

// specBytes is the number of bytes a Read Var item asks for: two per timer or
// counter, one per byte otherwise.
func specBytes(s wire.VarSpec) int {
	if wire.IsElementArea(s.TransportSize) {
		return int(s.Count) * wire.ElementAreaBytes
	}
	return int(s.Count)
}

// okItem is a successful Read Var item holding n bytes of fill.
func okItem(n int, fill byte) wire.ReadVarResult {
	return wire.ReadVarResult{ReturnCode: wire.RetCodeSuccess, TransportSize: wire.DataTransportSizeByte, RawLength: uint16(n * 8), Data: bytes.Repeat([]byte{fill}, n)}
}

// readAnswer encodes a Read Var answer of items.
func readAnswer(t *testing.T, ref uint16, items []wire.ReadVarResult) []byte {
	t.Helper()
	b, err := wire.EncodeReadVarResponse(ref, items)
	if err != nil {
		t.Errorf("EncodeReadVarResponse: %v", err)
	}
	return b
}

// answerAll answers every item in full, each byte the item's index.
func answerAll(t *testing.T) func(uint16, []wire.VarSpec) []byte {
	return func(ref uint16, specs []wire.VarSpec) []byte {
		items := make([]wire.ReadVarResult, len(specs))
		for i, s := range specs {
			items[i] = okItem(specBytes(s), byte(i))
		}
		return readAnswer(t, ref, items)
	}
}

// writeCodes answers a Write Var request with codes.
func writeCodes(t *testing.T, ref uint16, codes []byte) []byte {
	t.Helper()
	b, err := wire.EncodeWriteVarResponse(ref, codes)
	if err != nil {
		t.Errorf("EncodeWriteVarResponse: %v", err)
	}
	return b
}

func oneByteAddrs(n int) []model.Address {
	addrs := make([]model.Address, n)
	for i := range addrs {
		addrs[i] = model.Address{Area: model.AreaDB, DBNumber: 1, Start: 2 * i, Size: 1}
	}
	return addrs
}

func oneByteItems(n int) []WriteItem {
	items := make([]WriteItem, n)
	for i := range items {
		items[i] = WriteItem{Addr: model.Address{Area: model.AreaDB, DBNumber: 1, Start: 2 * i}, Data: []byte{byte(i)}}
	}
	return items
}

// A request holds at most maxItemsPerRequest items, even when the PDU has room
// for more: 45 one-byte items in a PDU of 480 bytes (room for 39 in a request)
// go as 20, 20 and 5.
func TestMultiCapsTheItemsPerRequest(t *testing.T) {
	m := &multiPeer{read: answerAll(t), write: func(ref uint16, n int) []byte {
		return writeCodes(t, ref, bytes.Repeat([]byte{wire.RetCodeSuccess}, n))
	}}
	c := multiClient(t, m)
	res, err := c.ReadMulti(context.Background(), oneByteAddrs(45))
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range res {
		if !r.OK() || len(r.Data) != 1 {
			t.Fatalf("address %d: %+v", i, r)
		}
	}
	errs, err := c.WriteMulti(context.Background(), oneByteItems(45))
	if err != nil || slices.ContainsFunc(errs, func(e error) bool { return e != nil }) {
		t.Fatalf("WriteMulti: %v, %v", errs, err)
	}
	if got, want := m.requests(), []int{20, 20, 5, 20, 20, 5}; !slices.Equal(got, want) {
		t.Errorf("items per request %v, want %v", got, want)
	}
}

// An answer that does not match its request fails the items of that request
// and no other, and the session goes on: a wrong item count, an item longer
// than asked or an answer that does not parse is a protocol error; a shorter
// item is a short read; a refused item is refused.
func TestReadMultiAnswersThatDoNotMatch(t *testing.T) {
	cases := []struct {
		name   string
		answer func(t *testing.T, ref uint16, specs []wire.VarSpec) []byte
		want   []ReadStatus // the first request's two items
	}{
		{"one item fewer", func(t *testing.T, ref uint16, specs []wire.VarSpec) []byte {
			return readAnswer(t, ref, []wire.ReadVarResult{okItem(specBytes(specs[0]), 1)})
		}, []ReadStatus{ReadStatusProtocolErr, ReadStatusProtocolErr}},
		{"item longer than asked", func(t *testing.T, ref uint16, specs []wire.VarSpec) []byte {
			return readAnswer(t, ref, []wire.ReadVarResult{okItem(specBytes(specs[0])+1, 1), okItem(specBytes(specs[1]), 2)})
		}, []ReadStatus{ReadStatusProtocolErr, ReadStatusSuccess}},
		{"item shorter than asked", func(t *testing.T, ref uint16, specs []wire.VarSpec) []byte {
			return readAnswer(t, ref, []wire.ReadVarResult{okItem(specBytes(specs[0])-1, 1), okItem(specBytes(specs[1]), 2)})
		}, []ReadStatus{ReadStatusShortRead, ReadStatusSuccess}},
		{"item refused", func(t *testing.T, ref uint16, specs []wire.VarSpec) []byte {
			return readAnswer(t, ref, []wire.ReadVarResult{okItem(specBytes(specs[0]), 1), {ReturnCode: wire.RetCodeNotAvailable}})
		}, []ReadStatus{ReadStatusSuccess, ReadStatusRejected}},
		{"answer does not parse", func(t *testing.T, ref uint16, specs []wire.VarSpec) []byte {
			return buildAckData(ref, 0, 0, []byte{wire.FuncReadVar, 2}, []byte{wire.RetCodeSuccess, wire.DataTransportSizeByte, 0xFF})
		}, []ReadStatus{ReadStatusProtocolErr, ReadStatusProtocolErr}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first := true
			m := &multiPeer{read: func(ref uint16, specs []wire.VarSpec) []byte {
				if first {
					first = false
					return tc.answer(t, ref, specs)
				}
				return answerAll(t)(ref, specs)
			}}
			c := multiClient(t, m)
			res, err := c.ReadMulti(context.Background(), []model.Address{
				{Area: model.AreaDB, DBNumber: 1, Size: 4},
				{Area: model.AreaTimer, Start: 2, Size: 1},
			})
			if err != nil {
				t.Fatal(err)
			}
			for i, want := range tc.want {
				if res[i].Status != want {
					t.Errorf("address %d: %+v, want %s", i, res[i], want)
				}
				if want == ReadStatusProtocolErr && !errors.Is(res[i].Err(), ErrProtocolFailure) {
					t.Errorf("address %d: Err() = %v, want ErrProtocolFailure", i, res[i].Err())
				}
			}
			if want := []int{2}; !slices.Equal(m.requests(), want) {
				t.Errorf("items per request %v, want %v", m.requests(), want)
			}
			if res, err := c.ReadMulti(context.Background(), oneByteAddrs(2)); err != nil || !res[0].OK() || !res[1].OK() {
				t.Errorf("next ReadMulti: %+v, %v; want the session open", res, err)
			}
		})
	}
}

// A transport failure fails the items of the request in flight and every item
// after it with a transport error, and ends the session; the items read
// before keep their data.
func TestReadMultiTransportFailure(t *testing.T) {
	addrs := append(oneByteAddrs(20), model.Address{Area: model.AreaDB, DBNumber: 1, Start: 100, Size: 2}, model.Address{Area: model.AreaMerkers, Size: 1})
	n := 0
	m := &multiPeer{read: func(ref uint16, specs []wire.VarSpec) []byte {
		if n++; n > 1 {
			return nil
		}
		return answerAll(t)(ref, specs)
	}}
	c := multiClient(t, m)
	res, err := c.ReadMulti(context.Background(), addrs)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range res {
		if i < 20 {
			if !r.OK() {
				t.Errorf("address %d read before the failure: %+v", i, r)
			}
			continue
		}
		if r.Status != ReadStatusTransportErr {
			t.Errorf("address %d: %+v, want transport-error", i, r)
		}
	}
	requireDisconnected(t, c)
}

// An area too large for one answer is read alone in chunks; when the
// transport fails in one of its chunks, it and every address after it fail.
func TestReadMultiTransportFailureInALargeArea(t *testing.T) {
	n := 0
	m := &multiPeer{read: func(ref uint16, specs []wire.VarSpec) []byte {
		if n++; n > 1 {
			return nil
		}
		return answerAll(t)(ref, specs)
	}}
	c := multiClient(t, m)
	res, err := c.ReadMulti(context.Background(), []model.Address{
		{Area: model.AreaDB, DBNumber: 1, Size: 600},
		{Area: model.AreaDB, DBNumber: 1, Start: 700, Size: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range res {
		if r.Status != ReadStatusTransportErr {
			t.Errorf("address %d: %+v, want transport-error", i, r)
		}
	}
	requireDisconnected(t, c)
}

// A Write Var answer that does not match its request fails the call with
// ErrProtocolFailure, which every item of that request and after it carries;
// a refused item carries its code; a transport failure ends the session.
func TestWriteMultiAnswersThatDoNotMatch(t *testing.T) {
	cases := []struct {
		name      string
		answer    func(t *testing.T, ref uint16, n int) []byte
		wantCall  error
		wantItems []func(error) bool
	}{
		{"one code fewer", func(t *testing.T, ref uint16, n int) []byte {
			return writeCodes(t, ref, bytes.Repeat([]byte{wire.RetCodeSuccess}, n-1))
		}, ErrProtocolFailure, nil},
		{"answer does not parse", func(t *testing.T, ref uint16, n int) []byte {
			return buildAckData(ref, 0, 0, []byte{wire.FuncReadVar, byte(n)}, bytes.Repeat([]byte{wire.RetCodeSuccess}, n))
		}, ErrProtocolFailure, nil},
		{"connection ends", func(*testing.T, uint16, int) []byte { return nil }, ErrNotConnected, nil},
		{"item refused", func(t *testing.T, ref uint16, n int) []byte {
			return writeCodes(t, ref, []byte{wire.RetCodeSuccess, wire.RetCodeAccessFault})
		}, nil, []func(error) bool{
			func(e error) bool { return e == nil },
			func(e error) bool { _, ok := errors.AsType[*wire.S7Error](e); return ok },
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &multiPeer{write: func(ref uint16, n int) []byte { return tc.answer(t, ref, n) }}
			c := multiClient(t, m)
			errs, err := c.WriteMulti(context.Background(), oneByteItems(2))
			if tc.wantCall == nil {
				if err != nil {
					t.Fatalf("WriteMulti: %v", err)
				}
				for i, ok := range tc.wantItems {
					if !ok(errs[i]) {
						t.Errorf("item %d: %v", i, errs[i])
					}
				}
				return
			}
			if tc.wantCall == ErrNotConnected {
				// The transport failure itself; the session has ended.
				if err == nil || !errors.Is(errs[0], err) || !errors.Is(errs[1], err) {
					t.Errorf("WriteMulti: %v, %v; want the transport failure on both items", errs, err)
				}
				requireDisconnected(t, c)
				return
			}
			if !errors.Is(err, tc.wantCall) || !errors.Is(errs[0], tc.wantCall) || !errors.Is(errs[1], tc.wantCall) {
				t.Errorf("WriteMulti: %v, %v; want %v on the call and both items", errs, err, tc.wantCall)
			}
		})
	}
}

// A job the PLC refuses in the answer's header (an error class) is a refusal,
// not a protocol failure: ReadMulti reports its items refused or failed with
// that error, WriteMulti returns it, and the session goes on.
func TestMultiJobRefusedInTheHeader(t *testing.T) {
	refuse := func(ref uint16) []byte { return buildAckData(ref, 0x81, 0x04, []byte{wire.FuncWriteVar, 0}, nil) }
	m := &multiPeer{
		read:  func(ref uint16, _ []wire.VarSpec) []byte { return refuse(ref) },
		write: func(ref uint16, _ int) []byte { return refuse(ref) },
	}
	c := multiClient(t, m)
	errs, err := c.WriteMulti(context.Background(), oneByteItems(2))
	s7, ok := errors.AsType[*wire.S7Error](err)
	if !ok || s7.Class != 0x81 || errors.Is(err, ErrProtocolFailure) || !errors.Is(errs[1], err) {
		t.Errorf("WriteMulti: %v, %v; want the header refusal, not a protocol failure", errs, err)
	}
	res, err := c.ReadMulti(context.Background(), oneByteAddrs(2))
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range res {
		if s7, ok := errors.AsType[*wire.S7Error](r.Err()); r.OK() || !ok || s7.Class != 0x81 || errors.Is(r.Err(), ErrProtocolFailure) {
			t.Errorf("address %d: %+v; want the header refusal, not a protocol failure", i, r)
		}
	}
	if c.PDUSize() == 0 {
		t.Error("the session ended; want it open after a refusal")
	}
}

// An address of size zero before one whose request fails still has a result:
// ReadMulti never returns a nil *ReadResult.
func TestReadMultiLeavesNoResultNil(t *testing.T) {
	m := &multiPeer{read: func(uint16, []wire.VarSpec) []byte { return nil }}
	c := multiClient(t, m)
	res, err := c.ReadMulti(context.Background(), []model.Address{
		{Area: model.AreaDB, DBNumber: 1, Size: 0},
		{Area: model.AreaDB, DBNumber: 1, Start: 2, Size: 2},
		{Area: model.AreaDB, DBNumber: 1, Start: 8, Size: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range res {
		if r == nil {
			t.Fatalf("address %d: nil result", i)
		}
	}
	if !res[0].OK() || res[1].Status != ReadStatusTransportErr || !res[2].OK() {
		t.Errorf("results %+v %+v %+v; want success, transport-error, success", res[0], res[1], res[2])
	}
}
