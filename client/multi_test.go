// SPDX-License-Identifier: MIT

package client_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/otfabric/go-s7comm/client"
	"github.com/otfabric/go-s7comm/model"
	"github.com/otfabric/go-s7comm/server"
	"github.com/otfabric/go-s7comm/wire"
)

// multiPLC is a test server with DB1 (1,000 bytes, counting), read-only DB2,
// markers, inputs, timers T0..T3 and counters C0..C1, granting pduSize.
func multiPLC(t *testing.T, pduSize int) (*server.Server, *client.Client) {
	t.Helper()
	srv, err := server.New(server.Config{MaxPDUSize: pduSize})
	if err != nil {
		t.Fatal(err)
	}
	db1 := make([]byte, 1000)
	for i := range db1 {
		db1[i] = byte(i)
	}
	for _, err := range []error{
		srv.RegisterDB(1, db1, server.AreaOptions{}),
		srv.RegisterDB(2, []byte{0xAA, 0xBB}, server.AreaOptions{ReadOnly: true}),
		srv.RegisterArea(server.AreaMerkers, make([]byte, 64), server.AreaOptions{}),
		srv.RegisterArea(server.AreaInputs, []byte{0x11, 0x22, 0x33}, server.AreaOptions{}),
		srv.RegisterArea(server.AreaTimers, []byte{0x25, 0x85, 0x20, 0x00, 0x00, 0x11, 0x12, 0x34}, server.AreaOptions{}),
		srv.RegisterArea(server.AreaCounters, []byte{0x00, 0x17, 0x00, 0x11}, server.AreaOptions{}),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background(), ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		if err := <-done; !errors.Is(err, server.ErrServerClosed) {
			t.Errorf("Serve = %v", err)
		}
	})
	c := client.New("127.0.0.1", client.WithPort(ln.Addr().(*net.TCPAddr).Port), client.WithRackSlot(0, 2), client.WithTimeout(2*time.Second))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return srv, c
}

func TestTimersAndCountersByElement(t *testing.T) {
	_, c := multiPLC(t, 240)
	ctx := context.Background()
	res, err := c.ReadArea(ctx, model.Address{Area: model.AreaTimer, Start: 1, Size: 3})
	if err != nil || !res.OK() || !bytes.Equal(res.Data, []byte{0x20, 0x00, 0x00, 0x11, 0x12, 0x34}) || res.RequestedLength != 6 {
		t.Fatalf("T1..T3 = %+v, %v", res, err)
	}
	res, err = c.ReadArea(ctx, model.Address{Area: model.AreaCounter, Start: 1, Size: 1})
	if err != nil || !res.OK() || !bytes.Equal(res.Data, []byte{0x00, 0x11}) {
		t.Fatalf("C1 = %+v, %v", res, err)
	}
	res, err = c.ReadArea(ctx, model.Address{Area: model.AreaTimer, Start: 3, Size: 2})
	if err != nil || res.Status != client.ReadStatusRejected {
		t.Fatalf("T3..T4 past the end = %+v, %v", res, err)
	}
	// A CPU refuses timer and counter writes; the request is the one it answers.
	var s7 *wire.S7Error
	if err := c.WriteArea(ctx, model.Address{Area: model.AreaCounter, Start: 0}, []byte{0, 5}); !errors.As(err, &s7) || s7.Code != wire.RetCodeAccessFault {
		t.Fatalf("write C0 = %v, want the access fault", err)
	}
	var verr *client.ValidationError
	if err := c.WriteArea(ctx, model.Address{Area: model.AreaTimer}, []byte{1}); !errors.As(err, &verr) {
		t.Fatalf("odd timer write = %v, want a ValidationError", err)
	}
}

func TestReadMulti(t *testing.T) {
	_, c := multiPLC(t, 240)
	addrs := []model.Address{
		{Area: model.AreaDB, DBNumber: 1, Start: 10, Size: 4},
		{Area: model.AreaMerkers, Start: 0, Size: 0},
		{Area: model.AreaInputs, Start: 1, Size: 2},
		{Area: model.AreaDB, DBNumber: 9, Start: 0, Size: 2},
		{Area: model.AreaTimer, Start: 0, Size: 2},
		{Area: model.AreaCounter, Start: 1, Size: 1},
		{Area: model.AreaDB, DBNumber: 1, Start: 100, Size: 600},
		{Area: model.AreaDB, DBNumber: 1, Start: 200, Size: 101},
		{Area: model.AreaDB, DBNumber: 1, Start: 400, Size: 101},
		{Area: model.AreaDB, DBNumber: 2, Start: 0, Size: 4},
	}
	res, err := c.ReadMulti(context.Background(), addrs)
	if err != nil || len(res) != len(addrs) {
		t.Fatalf("ReadMulti: %v (%d results)", err, len(res))
	}
	db := func(off, n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(off + i)
		}
		return b
	}
	want := [][]byte{db(10, 4), {}, {0x22, 0x33}, nil, {0x25, 0x85, 0x20, 0x00}, {0x00, 0x11}, db(100, 600), db(200, 101), db(400, 101), nil}
	for i, r := range res {
		switch i {
		case 3:
			if r.Status != client.ReadStatusRejected || r.ReturnCode != wire.RetCodeNotAvailable {
				t.Errorf("DB9: %+v, want refused as not available", r)
			}
		case 9:
			if r.Status != client.ReadStatusRejected {
				t.Errorf("DB2 past its end: %+v, want refused", r)
			}
		default:
			if !r.OK() || !bytes.Equal(r.Data, want[i]) {
				t.Errorf("address %d: %+v", i, r)
			}
		}
	}
	if res, err := c.ReadMulti(context.Background(), nil); err != nil || len(res) != 0 {
		t.Errorf("no addresses: %v, %v", res, err)
	}
	var verr *client.ValidationError
	if _, err := c.ReadMulti(context.Background(), []model.Address{{Area: model.AreaDB, DBNumber: 1, Start: -1, Size: 1}}); !errors.As(err, &verr) {
		t.Errorf("bad start = %v, want a ValidationError", err)
	}
	if _, err := c.ReadMulti(context.Background(), []model.Address{{Area: model.AreaDB, DBNumber: 1, Start: wire.MaxS7AnyByteOffset, Size: 4}}); !errors.As(err, &verr) {
		t.Errorf("past the address space = %v, want a ValidationError", err)
	}
}

func TestReadMultiPacksManyItemsInOneRequest(t *testing.T) {
	_, c := multiPLC(t, 960)
	addrs := make([]model.Address, 60)
	for i := range addrs {
		addrs[i] = model.Address{Area: model.AreaDB, DBNumber: 1, Start: 3 * i, Size: 1}
	}
	res, err := c.ReadMulti(context.Background(), addrs)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range res {
		if !r.OK() || len(r.Data) != 1 || r.Data[0] != byte(3*i) {
			t.Fatalf("address %d: %+v", i, r)
		}
	}
}

func TestReadMultiEndedContextAndClosedClient(t *testing.T) {
	_, c := multiPLC(t, 240)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	addrs := []model.Address{{Area: model.AreaDB, DBNumber: 1, Size: 2}, {Area: model.AreaTimer, Size: 1}}
	res, err := c.ReadMulti(ctx, addrs)
	if err != nil || len(res) != 2 || res[0].OK() || res[1].OK() || res[1].RequestedLength != 2 {
		t.Fatalf("ended context: %+v, %v", res, err)
	}
	_ = c.Close()
	res, err = c.ReadMulti(context.Background(), addrs)
	if err != nil || res[0].OK() || !errors.Is(res[0].Err(), client.ErrNotConnected) {
		t.Fatalf("closed client: %+v, %v", res[0], err)
	}
}

func TestWriteMulti(t *testing.T) {
	srv, c := multiPLC(t, 240)
	ctx := context.Background()
	big := bytes.Repeat([]byte{0x5A}, 500)
	items := []client.WriteItem{
		{Addr: model.Address{Area: model.AreaDB, DBNumber: 1, Start: 0}, Data: []byte{0xDE, 0xAD, 0xBE}},
		{Addr: model.Address{Area: model.AreaMerkers, Start: 3}, Data: []byte{0x42}},
		{Addr: model.Address{Area: model.AreaDB, DBNumber: 2, Start: 0}, Data: []byte{0x01}},
		{Addr: model.Address{Area: model.AreaMerkers, Start: 9}, Data: nil},
		{Addr: model.Address{Area: model.AreaDB, DBNumber: 1, Start: 400}, Data: big},
		{Addr: model.Address{Area: model.AreaCounter, Start: 0}, Data: []byte{0, 7}},
		{Addr: model.Address{Area: model.AreaDB, DBNumber: 2, Start: 0}, Data: bytes.Repeat([]byte{1}, 300)},
	}
	errs, err := c.WriteMulti(ctx, items)
	if err != nil || len(errs) != len(items) {
		t.Fatalf("WriteMulti: %v (%d outcomes)", err, len(errs))
	}
	var s7 *wire.S7Error
	for i, e := range errs {
		switch i {
		case 2, 5, 6:
			if !errors.As(e, &s7) {
				t.Errorf("item %d = %v, want the PLC's refusal", i, e)
			}
		default:
			if e != nil {
				t.Errorf("item %d = %v", i, e)
			}
		}
	}
	for _, chk := range []struct {
		area      server.Area
		db, start int
		want      []byte
	}{{server.AreaDB, 1, 0, []byte{0xDE, 0xAD, 0xBE}}, {server.AreaMerkers, 0, 3, []byte{0x42}}, {server.AreaDB, 1, 400, big}} {
		if got, _ := srv.Read(chk.area, chk.db, chk.start, len(chk.want)); !bytes.Equal(got, chk.want) {
			t.Errorf("%s %d at %d = %x", chk.area, chk.db, chk.start, got)
		}
	}

	if errs, err := c.WriteMulti(ctx, nil); err != nil || len(errs) != 0 {
		t.Errorf("no items: %v, %v", errs, err)
	}
	var verr *client.ValidationError
	for name, it := range map[string]client.WriteItem{
		"bad start": {Addr: model.Address{Area: model.AreaDB, DBNumber: 1, Start: -1}, Data: []byte{1}},
		"odd timer": {Addr: model.Address{Area: model.AreaTimer}, Data: []byte{1}},
		"past end":  {Addr: model.Address{Area: model.AreaMerkers, Start: wire.MaxS7AnyByteOffset}, Data: []byte{1, 2}},
	} {
		if _, err := c.WriteMulti(ctx, []client.WriteItem{it}); !errors.As(err, &verr) {
			t.Errorf("%s = %v, want a ValidationError", name, err)
		}
	}
}

func TestWriteMultiPacksAndFailsTheRest(t *testing.T) {
	srv, c := multiPLC(t, 480)
	ctx := context.Background()
	items := make([]client.WriteItem, 30)
	for i := range items {
		items[i] = client.WriteItem{Addr: model.Address{Area: model.AreaDB, DBNumber: 1, Start: 2 * i}, Data: []byte{byte(100 + i)}}
	}
	if errs, err := c.WriteMulti(ctx, items); err != nil {
		t.Fatalf("WriteMulti: %v %v", err, errs)
	}
	for i := range items {
		if got, _ := srv.Read(server.AreaDB, 1, 2*i, 1); got[0] != byte(100+i) {
			t.Fatalf("DB1 byte %d = %d", 2*i, got[0])
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	errs, err := c.WriteMulti(cctx, items[:2])
	if !errors.Is(err, context.Canceled) || !errors.Is(errs[1], context.Canceled) {
		t.Fatalf("ended context: %v, %v", errs, err)
	}
	_ = c.Close()
	errs, err = c.WriteMulti(ctx, items[:2])
	if !errors.Is(err, client.ErrNotConnected) || !errors.Is(errs[0], client.ErrNotConnected) {
		t.Fatalf("closed client: %v, %v", errs, err)
	}
}

func TestReadMultiAndWriteMultiWithRateLimit(t *testing.T) {
	_, c := multiPLC(t, 240)
	c2 := client.New("127.0.0.1", client.WithPort(c.ConnectionInfo().Port), client.WithRackSlot(0, 2), client.WithRateLimit(time.Millisecond))
	if err := c2.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	addrs := []model.Address{{Area: model.AreaDB, DBNumber: 1, Size: 400}, {Area: model.AreaDB, DBNumber: 1, Start: 500, Size: 200}, {Area: model.AreaDB, DBNumber: 1, Start: 800, Size: 2}}
	res, err := c2.ReadMulti(context.Background(), addrs)
	if err != nil || !res[0].OK() || !res[1].OK() || !res[2].OK() {
		t.Fatalf("ReadMulti: %+v, %v", res, err)
	}
	errs, err := c2.WriteMulti(context.Background(), []client.WriteItem{
		{Addr: model.Address{Area: model.AreaMerkers}, Data: bytes.Repeat([]byte{1}, 40)},
		{Addr: model.Address{Area: model.AreaDB, DBNumber: 1, Start: 100}, Data: bytes.Repeat([]byte{2}, 400)},
		{Addr: model.Address{Area: model.AreaMerkers, Start: 50}, Data: []byte{3}},
	})
	if err != nil || errs[0] != nil || errs[1] != nil || errs[2] != nil {
		t.Fatalf("WriteMulti: %v, %v", errs, err)
	}
}
