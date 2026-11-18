// SPDX-License-Identifier: MIT

package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/otfabric/go-s7comm/model"
	"github.com/otfabric/go-s7comm/wire"
)

// readItemAnswer is at most what one item takes in a Read Var answer: its
// 4-byte header and its data with its pad byte.
func readItemAnswer(n int) int { return 4 + n + n%2 }

// ReadMulti reads several areas, packing as many as fit the negotiated PDU into
// one Read Var request: the request holds 12 bytes per item, the answer 4 bytes
// and the data of each, padded to an even length. An area whose answer does not
// fit one PDU is read alone, chunked as ReadArea reads it; one of size zero is
// not sent. It returns one result per address, in order, each as ReadArea
// returns it (timers and counters are addressed by element). An invalid
// address returns an error and reads nothing. A transport failure or the end
// of ctx fails the result of every address not read yet.
func (c *Client) ReadMulti(ctx context.Context, addrs []model.Address) ([]*ReadResult, error) {
	for i, a := range addrs {
		if err := validateAddress(a); err != nil {
			return nil, &ValidationError{Message: fmt.Sprintf("address %d: %v", i, err)}
		}
		if err := validateRangeEnd(a.Start, a.Size); err != nil {
			return nil, &ValidationError{Message: fmt.Sprintf("address %d: %v", i, err)}
		}
	}
	out := make([]*ReadResult, len(addrs))
	if len(addrs) == 0 {
		return out, nil
	}
	if err := c.beginCall(ctx); err != nil {
		failRest(out, addrs, 0, err)
		return out, nil
	}
	defer c.endCall()

	c.mu.RLock()
	pduSize := c.pduSize
	rateLimit := c.opts.rateLimit
	c.mu.RUnlock()

	first := true
	for _, g := range planReadGroups(addrs, pduSize) {
		if len(g) == 1 && readItemAnswer(addrs[g[0]].Size*unitBytes(addrs[g[0]].Area)) > maxReadAnswer(pduSize) {
			res, sent, err := c.readArea(ctx, addrs[g[0]], pduSize, rateLimit, first)
			if err != nil {
				return nil, err
			}
			first = first && !sent
			out[g[0]] = res
			if res.Cause != nil && res.Status != ReadStatusProtocolErr {
				failRest(out, addrs, g[0]+1, res.Cause)
				return out, nil
			}
			continue
		}
		if err := ctx.Err(); err != nil {
			failRest(out, addrs, g[0], err)
			return out, nil
		}
		if rateLimit > 0 && !first {
			if err := c.ratePause(ctx, rateLimit); err != nil {
				failRest(out, addrs, g[0], err)
				return out, nil
			}
		}
		first = false
		if err := c.readGroup(ctx, addrs, g, out); err != nil {
			failRest(out, addrs, g[0], err)
			return out, nil
		}
	}
	for i, a := range addrs {
		if out[i] == nil && a.Size == 0 {
			out[i] = &ReadResult{Status: ReadStatusSuccess}
		}
	}
	return out, nil
}

// maxReadAnswer is the room for items in a Read Var answer of one PDU.
func maxReadAnswer(pduSize int) int { return pduSize - wire.AckDataHeaderSize - 2 }

// planReadGroups packs the addresses (by index, in order) into Read Var
// requests that fit the PDU; an address too large for one answer is a group of
// its own, and one of size zero is in none.
func planReadGroups(addrs []model.Address, pduSize int) [][]int {
	maxReq := pduSize - wire.S7HeaderSize - 2
	maxAns := maxReadAnswer(pduSize)
	var groups [][]int
	var cur []int
	req, ans := 0, 0
	for i, a := range addrs {
		if a.Size == 0 {
			continue
		}
		need := readItemAnswer(a.Size * unitBytes(a.Area))
		if need > maxAns {
			groups = append(groups, []int{i})
			continue
		}
		if len(cur) > 0 && (req+12 > maxReq || ans+need > maxAns || len(cur) == wire.MaxReadVarItems) {
			groups = append(groups, cur)
			cur, req, ans = nil, 0, 0
		}
		cur = append(cur, i)
		req += 12
		ans += need
	}
	if len(cur) > 0 {
		groups = append(groups, cur)
	}
	return groups
}

// readGroup sends one Read Var request for the addresses g and sets their
// results. A non-nil error is a transport failure: the session has ended.
func (c *Client) readGroup(ctx context.Context, addrs []model.Address, g []int, out []*ReadResult) error {
	specs := make([]wire.S7AnyAddress, len(g))
	for k, i := range g {
		specs[k] = wire.S7AnyAddress{Area: byte(addrs[i].Area), DBNumber: addrs[i].DBNumber, Start: addrs[i].Start, Size: addrs[i].Size}
	}
	ref := c.nextPDURef()
	req, err := wire.EncodeReadVarRequest(ref, specs)
	if err != nil {
		return err
	}
	param, data, err := c.sendReceive(ctx, req, ref)
	if err != nil {
		return err
	}
	items, err := wire.ParseReadVarResponse(param, data)
	if err == nil && len(items) != len(g) {
		err = fmt.Errorf("answer holds %d items for %d: %w", len(items), len(g), ErrProtocolFailure)
	}
	for k, i := range g {
		want := addrs[i].Size * unitBytes(addrs[i].Area)
		res := &ReadResult{RequestedLength: want}
		out[i] = res
		if err != nil {
			res.Status, res.Message, res.Cause = ReadStatusProtocolErr, err.Error(), err
			continue
		}
		item := items[k]
		if rerr := wire.ReturnCodeError(item.ReturnCode); rerr != nil {
			res.Status, res.ReturnCode, res.Message = ReadStatusRejected, item.ReturnCode, rerr.Error()
			res.ItemStatus = wire.ItemReturnCodeString(item.ReturnCode)
			continue
		}
		if len(item.Data) > want {
			res.Status = ReadStatusProtocolErr
			res.Message = fmt.Sprintf("item returned %d bytes for %d requested", len(item.Data), want)
			res.Cause = fmt.Errorf("%s: %w", res.Message, ErrProtocolFailure)
			continue
		}
		finishRead(res, append([]byte(nil), item.Data...))
	}
	return nil
}

// failRest fails the result of every address from index from on that has none.
func failRest(out []*ReadResult, addrs []model.Address, from int, err error) {
	for i := from; i < len(addrs); i++ {
		if out[i] == nil {
			out[i] = newFailedReadResult(max(addrs[i].Size, 0)*unitBytes(addrs[i].Area), err)
		}
	}
}

// WriteItem is one area and the bytes to write there, for WriteMulti (two bytes
// per element on the timer and counter areas).
type WriteItem struct {
	Addr model.Address
	Data []byte
}

// WriteMulti writes several areas, packing as many items as fit the negotiated
// PDU into one Write Var request. An item too large for one request is written
// alone, chunked as WriteArea writes it; one with no data is not sent. It
// returns one error per item, in order: nil when written, the item's return
// code as an error (ReturnCodeError) when the PLC refused it. An invalid item
// returns an error and writes nothing. A transport failure or the end of ctx
// returns that error, and every item not written yet carries it too.
func (c *Client) WriteMulti(ctx context.Context, items []WriteItem) ([]error, error) {
	for i, it := range items {
		if err := validateAddress(it.Addr); err != nil {
			return nil, &ValidationError{Message: fmt.Sprintf("item %d: %v", i, err)}
		}
		unit := unitBytes(it.Addr.Area)
		if len(it.Data)%unit != 0 {
			return nil, &ValidationError{Message: fmt.Sprintf("item %d: %d bytes for %s: two per timer or counter", i, len(it.Data), it.Addr.Area)}
		}
		if err := validateRangeEnd(it.Addr.Start, len(it.Data)/unit); err != nil {
			return nil, &ValidationError{Message: fmt.Sprintf("item %d: %v", i, err)}
		}
	}
	out := make([]error, len(items))
	if len(items) == 0 {
		return out, nil
	}
	if err := c.beginCall(ctx); err != nil {
		return failWrites(out, 0, err), err
	}
	defer c.endCall()

	c.mu.RLock()
	pduSize := c.pduSize
	rateLimit := c.opts.rateLimit
	c.mu.RUnlock()

	maxReq := pduSize - wire.S7HeaderSize - 2
	first := true
	for _, g := range planWriteGroups(items, pduSize) {
		if err := ctx.Err(); err != nil {
			return failWrites(out, g[0], err), err
		}
		if len(g) == 1 && wire.WriteVarItemSize(len(items[g[0]].Data)) > maxReq {
			err := c.writeArea(ctx, items[g[0]].Addr, items[g[0]].Data, pduSize, rateLimit, first)
			first = false
			if err != nil && !isRefusal(err) {
				return failWrites(out, g[0], err), err
			}
			out[g[0]] = err
			continue
		}
		if rateLimit > 0 && !first {
			if err := c.ratePause(ctx, rateLimit); err != nil {
				return failWrites(out, g[0], err), err
			}
		}
		first = false
		if err := c.writeGroup(ctx, items, g, out); err != nil {
			return failWrites(out, g[0], err), err
		}
	}
	return out, nil
}

// planWriteGroups packs the items (by index, in order) into Write Var requests
// that fit the PDU; an item too large for one request is a group of its own,
// and one with no data is in none.
func planWriteGroups(items []WriteItem, pduSize int) [][]int {
	maxReq := pduSize - wire.S7HeaderSize - 2
	var groups [][]int
	var cur []int
	size := 0
	for i, it := range items {
		if len(it.Data) == 0 {
			continue
		}
		need := wire.WriteVarItemSize(len(it.Data))
		if need > maxReq {
			groups = append(groups, []int{i})
			continue
		}
		if len(cur) > 0 && (size+need > maxReq || len(cur) == wire.MaxReadVarItems) {
			groups = append(groups, cur)
			cur, size = nil, 0
		}
		cur = append(cur, i)
		size += need
	}
	if len(cur) > 0 {
		groups = append(groups, cur)
	}
	return groups
}

// writeGroup sends one Write Var request for the items g and sets their
// errors. A non-nil error fails the whole request.
func (c *Client) writeGroup(ctx context.Context, items []WriteItem, g []int, out []error) error {
	wi := make([]wire.WriteVarItem, len(g))
	for k, i := range g {
		a := items[i].Addr
		wi[k] = wire.WriteVarItem{
			Addr:  wire.S7AnyAddress{Area: byte(a.Area), DBNumber: a.DBNumber, Start: a.Start, Size: len(items[i].Data) / unitBytes(a.Area)},
			Value: items[i].Data,
		}
	}
	ref := c.nextPDURef()
	req, err := wire.EncodeWriteVarItemsRequest(ref, wi)
	if err != nil {
		return err
	}
	param, data, err := c.sendReceive(ctx, req, ref)
	if err != nil {
		return err
	}
	codes, err := wire.ParseWriteVarResponseCodes(param, data)
	if err != nil {
		return err
	}
	if len(codes) != len(g) {
		return fmt.Errorf("answer holds %d item codes for %d: %w", len(codes), len(g), ErrProtocolFailure)
	}
	for k, i := range g {
		out[i] = wire.ReturnCodeError(codes[k])
	}
	return nil
}

// isRefusal reports whether a write failed because the PLC refused it: an item
// return code or an error class in its answer. The session goes on.
func isRefusal(err error) bool {
	var s7 *wire.S7Error
	return errors.As(err, &s7)
}

// failWrites sets err on every item from index from on that has no outcome yet.
func failWrites(out []error, from int, err error) []error {
	for i := from; i < len(out); i++ {
		if out[i] == nil {
			out[i] = err
		}
	}
	return out
}
