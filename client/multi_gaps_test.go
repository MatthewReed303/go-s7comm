// SPDX-License-Identifier: MIT

package client

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/otfabric/go-s7comm/model"
	"github.com/otfabric/go-s7comm/wire"
)

// A context whose deadline passes in the pause of WithRateLimit between two
// requests of ReadMulti or WriteMulti ends the call there: the items of the
// first request keep their outcome, the others fail with the deadline
// (reads: status timeout), nothing more is sent and the session stays open.
func TestMultiPauseBetweenRequestsEndsWithItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := &multiPeer{read: answerAll(t), write: func(ref uint16, n int) []byte {
			return writeCodes(t, ref, bytes.Repeat([]byte{wire.RetCodeSuccess}, n))
		}}
		c := multiClient(t, m, WithRateLimit(time.Hour))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		res, err := c.ReadMulti(ctx, oneByteAddrs(25))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		for i, r := range res {
			if i < maxItemsPerRequest != r.OK() || (i >= maxItemsPerRequest && r.Status != ReadStatusTimeout) {
				t.Errorf("address %d: %+v", i, r)
			}
		}

		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		errs, err := c.WriteMulti(ctx, oneByteItems(25))
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WriteMulti: %v, want context.DeadlineExceeded", err)
		}
		for i, e := range errs {
			if (i < maxItemsPerRequest) != (e == nil) {
				t.Errorf("item %d: %v", i, e)
			}
		}
		if got := m.requests(); len(got) != 2 {
			t.Errorf("requests %v, want one read and one write", got)
		}
		if c.PDUSize() == 0 {
			t.Error("the session ended; want it open")
		}
	})
}

// An address the encoder refuses is ruled out by the checks of the public
// calls; the internal steps still fail cleanly, without sending, should one
// reach them: readArea fails its result with a *ValidationError, writeArea,
// readGroup and writeGroup return one.
func TestEncoderRefusalsSendNothing(t *testing.T) {
	c := New("plc")
	bad := model.Address{Area: model.AreaDB, DBNumber: wire.MaxS7AnyDBNumber + 1, Size: 2}
	var verr *ValidationError
	if res, sent := c.readArea(context.Background(), bad, 480, 0, true); sent || !errors.As(res.Err(), &verr) {
		t.Errorf("readArea: %+v sent=%v, want a *ValidationError and nothing sent", res, sent)
	}
	if err := c.writeArea(context.Background(), bad, []byte{1, 2}, 480, 0, true); !errors.As(err, &verr) {
		t.Errorf("writeArea: %v, want a *ValidationError", err)
	}
	if err := c.readGroup(context.Background(), []model.Address{bad}, []int{0}, make([]*ReadResult, 1)); !errors.Is(err, wire.ErrOutOfRange) {
		t.Errorf("readGroup: %v, want ErrOutOfRange", err)
	}
	if err := c.writeGroup(context.Background(), []WriteItem{{Addr: bad, Data: []byte{1}}}, []int{0}, make([]error, 1)); !errors.Is(err, wire.ErrOutOfRange) {
		t.Errorf("writeGroup: %v, want ErrOutOfRange", err)
	}
}

// A pause of WithRateLimit that would start after Close returns ErrClosed at
// once, without waiting.
func TestRatePauseAfterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New("plc")
		if err := c.beginCall(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer c.endCall()
		_ = c.Close()
		start := time.Now()
		if err := c.ratePause(context.Background(), time.Hour); !errors.Is(err, ErrClosed) {
			t.Errorf("ratePause: %v, want ErrClosed", err)
		}
		if d := time.Since(start); d != 0 {
			t.Errorf("ratePause took %v, want no wait", d)
		}
	})
}

// fakeTimeout is a net.Error that timed out.
type fakeTimeout struct{}

func (fakeTimeout) Error() string   { return "i/o timeout" }
func (fakeTimeout) Timeout() bool   { return true }
func (fakeTimeout) Temporary() bool { return true }

// A transport timeout (a net.Error whose Timeout is true) is status timeout.
func TestClassifyOpErrorNetTimeout(t *testing.T) {
	err := &net.OpError{Op: "read", Net: "tcp", Err: fakeTimeout{}}
	if got := classifyOpError(err); got != ReadStatusTimeout {
		t.Errorf("classifyOpError(%v) = %s, want timeout", err, got)
	}
}

// WriteArea with a context that has already ended returns its error, sends
// nothing and leaves the session open.
func TestWriteAreaEndedContextSendsNothing(t *testing.T) {
	peers, requests := readWritePipePeers(t)
	c := pipeClient(t, peers)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.WriteDB(ctx, 1, 0, []byte{1}); !errors.Is(err, context.Canceled) {
		t.Errorf("WriteDB: %v, want context.Canceled", err)
	}
	if n := requests.Load(); n != 0 {
		t.Errorf("the peer received %d requests, want 0", n)
	}
	if c.PDUSize() == 0 {
		t.Error("the session ended; want it open")
	}
}

// errAfterContext is a context whose Err reports context.Canceled once ended
// is set, while Done never closes: an end that the client sees only where it
// asks Err, as between two chunks of a write.
type errAfterContext struct {
	context.Context
	ended *atomic.Bool
}

func (c errAfterContext) Err() error {
	if c.ended.Load() {
		return context.Canceled
	}
	return nil
}

// endingConn sets ended once the client has read an answer to a job.
type endingConn struct {
	net.Conn
	armed, ended *atomic.Bool
}

func (c endingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 && c.armed.Load() {
		c.ended.Store(true)
	}
	return n, err
}

// A context that ends between two chunks of a write (no rate limit) ends the
// write before the next chunk: the call returns the context's error, the peer
// received the first chunk only, and the session stays open, since no request
// is left unanswered.
func TestWriteAreaContextEndsBetweenChunks(t *testing.T) {
	peers, requests := readWritePipePeers(t)
	var armed, ended atomic.Bool
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := peers.dial(ctx, network, addr)
		return endingConn{Conn: conn, armed: &armed, ended: &ended}, err
	}
	c := New("plc", WithRackSlot(0, 1), WithDialer(dial))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	armed.Store(true)
	ctx := errAfterContext{Context: context.Background(), ended: &ended}
	if err := c.WriteDB(ctx, 1, 0, make([]byte, 500)); !errors.Is(err, context.Canceled) {
		t.Errorf("WriteDB: %v, want context.Canceled", err)
	}
	if n := requests.Load(); n != 1 {
		t.Errorf("the peer received %d requests, want 1", n)
	}
	if c.PDUSize() == 0 {
		t.Error("the session ended; want it open")
	}
}
