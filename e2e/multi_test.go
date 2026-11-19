// SPDX-License-Identifier: MIT

package e2e

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/otfabric/go-s7comm/client"
	"github.com/otfabric/go-s7comm/model"
)

// WriteMulti then ReadMulti over several areas, more items than one request
// takes and an area larger than the PDU: every address reads back what was
// written, and an address the PLC does not hold is refused alone.
func TestWriteMultiThenReadMultiBack(t *testing.T) {
	c := connect(t, startPLC(t, 240))
	ctx := context.Background()
	var items []client.WriteItem
	for i := range 25 {
		items = append(items, client.WriteItem{Addr: model.Address{Area: model.AreaDB, DBNumber: 1, Start: 4 * i}, Data: []byte{byte(i), byte(i + 1), byte(i + 2)}})
	}
	big := bytes.Repeat([]byte{0xA5}, 500)
	items = append(items,
		client.WriteItem{Addr: model.Address{Area: model.AreaMerkers, Start: 10}, Data: []byte{0x42}},
		client.WriteItem{Addr: model.Address{Area: model.AreaDB, DBNumber: 1, Start: 300}, Data: big},
		client.WriteItem{Addr: model.Address{Area: model.AreaDB, DBNumber: 7}, Data: []byte{1}},
		client.WriteItem{Addr: model.Address{Area: model.AreaOutputs, Start: 3}, Data: []byte{0x0F, 0xF0}},
	)
	errs, err := c.WriteMulti(ctx, items)
	if err != nil {
		t.Fatalf("WriteMulti: %v", err)
	}
	addrs := make([]model.Address, len(items))
	for i, it := range items {
		addrs[i] = it.Addr
		addrs[i].Size = len(it.Data)
		refused := it.Addr.DBNumber == 7
		if (errs[i] != nil) != refused {
			t.Errorf("item %d: %v", i, errs[i])
		}
	}
	res, err := c.ReadMulti(ctx, addrs)
	if err != nil {
		t.Fatalf("ReadMulti: %v", err)
	}
	for i, r := range res {
		if items[i].Addr.DBNumber == 7 {
			if r.Status != client.ReadStatusRejected {
				t.Errorf("DB7: %+v, want refused", r)
			}
			continue
		}
		if !r.OK() || !bytes.Equal(r.Data, items[i].Data) {
			t.Errorf("address %d: %+v, want %x", i, r, items[i].Data)
		}
	}
}

// Every answer to a multi-item job is corrupted (mutate). Each call must come
// back within its deadline, with one outcome per address or item, never more
// bytes than an address asked for, and never panic; goleak checks that no
// goroutine is left behind.
func TestHostilePLCAnswersToMultiItemJobs(t *testing.T) {
	p := startPLC(t, 240)
	c := connect(t, p)
	var mu sync.Mutex
	rng := rand.New(rand.NewPCG(5, 6)) // deterministic: a failure replays
	p.setRewrite(func(out []byte) []byte {
		mu.Lock()
		defer mu.Unlock()
		return mutate(rng, out)
	})

	ops := rand.New(rand.NewPCG(7, 8))
	for i := range 300 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()
		var err error
		n := 1 + ops.IntN(30)
		if ops.IntN(2) == 0 {
			addrs := make([]model.Address, n)
			for k := range addrs {
				addrs[k] = model.Address{Area: model.AreaDB, DBNumber: 1, Start: ops.IntN(500), Size: ops.IntN(1 + ops.IntN(400))}
			}
			var res []*client.ReadResult
			res, err = c.ReadMulti(ctx, addrs)
			if err != nil {
				t.Fatalf("op %d: ReadMulti returned %v for valid addresses", i, err)
			}
			if len(res) != n {
				t.Fatalf("op %d: %d results for %d addresses", i, len(res), n)
			}
			for k, r := range res {
				if len(r.Data) > addrs[k].Size {
					t.Fatalf("op %d: address %d returned %d bytes for %d asked", i, k, len(r.Data), addrs[k].Size)
				}
				if !r.OK() && errors.Is(r.Err(), client.ErrNotConnected) {
					err = r.Err()
				}
			}
		} else {
			items := make([]client.WriteItem, n)
			for k := range items {
				items[k] = client.WriteItem{Addr: model.Address{Area: model.AreaDB, DBNumber: 1, Start: ops.IntN(500)}, Data: make([]byte, ops.IntN(1+ops.IntN(300)))}
			}
			var errs []error
			errs, err = c.WriteMulti(ctx, items)
			if errs != nil && len(errs) != n {
				t.Fatalf("op %d: %d outcomes for %d items", i, len(errs), n)
			}
		}
		cancel()
		if d := time.Since(start); d > 3*time.Second {
			t.Fatalf("op %d took %v", i, d)
		}
		if err != nil || c.PDUSize() == 0 {
			// A corrupted stream may leave the connection unusable: reconnect.
			_ = c.Close()
			if cerr := c.Connect(context.Background()); cerr != nil {
				t.Fatalf("op %d: reconnect: %v", i, cerr)
			}
		}
	}
}
