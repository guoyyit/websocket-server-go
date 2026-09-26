package websocket

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestTextMessageRoundTrip(t *testing.T) {
	c, tc := newConnPair(t, quietConfig())

	if err := tc.writeFrame(true, OpText, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	m, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if m.Type != OpText || string(m.Data) != "hello" {
		t.Fatalf("message = %+v", m)
	}

	if err := c.WriteMessage(OpText, []byte("world")); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	tc.expectMessage(t, OpText, "world")
}

func TestBinaryMessageRoundTrip(t *testing.T) {
	c, tc := newConnPair(t, quietConfig())

	payload := []byte{0x00, 0x01, 0xFE, 0xFF}
	if err := tc.writeFrame(true, OpBinary, payload); err != nil {
		t.Fatal(err)
	}
	m, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if m.Type != OpBinary || !bytes.Equal(m.Data, payload) {
		t.Fatalf("message = %+v", m)
	}
}

func TestFragmentedMessageReassembled(t *testing.T) {
	c, tc := newConnPair(t, quietConfig())

	// "hello world" in three fragments.
	if err := tc.writeFrame(false, OpText, []byte("hello ")); err != nil {
		t.Fatal(err)
	}
	if err := tc.writeFrame(false, OpContinuation, []byte("wor")); err != nil {
		t.Fatal(err)
	}
	if err := tc.writeFrame(true, OpContinuation, []byte("ld")); err != nil {
		t.Fatal(err)
	}
	m, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(m.Data) != "hello world" {
		t.Fatalf("message = %q", m.Data)
	}
}

func TestControlFrameInterleavedWithFragments(t *testing.T) {
	c, tc := newConnPair(t, quietConfig())

	if err := tc.writeFrame(false, OpText, []byte("ab")); err != nil {
		t.Fatal(err)
	}
	// A ping may arrive between fragments and must be answered.
	if err := tc.writeFrame(true, OpPing, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := tc.writeFrame(true, OpContinuation, []byte("cd")); err != nil {
		t.Fatal(err)
	}

	h, payload, err := tc.readFrame()
	if err != nil {
		t.Fatal(err)
	}
	if h.op != OpPong || string(payload) != "x" {
		t.Fatalf("expected pong echo, got op=%v payload=%q", h.op, payload)
	}
	m, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(m.Data) != "abcd" {
		t.Fatalf("message = %q", m.Data)
	}
}

func TestServerFragmentsLargeOutboundMessage(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxFrameSize = 16
	c, tc := newConnPair(t, cfg)

	data := bytes.Repeat([]byte("abcdefghij"), 10) // 100 bytes
	if err := c.WriteMessage(OpBinary, data); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	var got []byte
	for i := 0; ; i++ {
		h, payload, err := tc.readFrame()
		if err != nil {
			t.Fatal(err)
		}
		wantOp := OpContinuation
		if i == 0 {
			wantOp = OpBinary
		}
		if h.op != wantOp {
			t.Fatalf("frame %d op = %v, want %v", i, h.op, wantOp)
		}
		if len(payload) > 16 {
			t.Fatalf("frame %d exceeds MaxFrameSize", i)
		}
		got = append(got, payload...)
		if h.fin {
			break
		}
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("reassembled payload mismatch")
	}
}

func TestUnmaskedClientFrameClosesWithProtocolError(t *testing.T) {
	_, tc := newConnPair(t, quietConfig())

	// Hand-craft an unmasked text frame.
	if _, err := tc.nc.Write([]byte{0x81, 0x02, 'h', 'i'}); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseProtocolError {
		t.Fatalf("close code = %d, want %d", code, CloseProtocolError)
	}
}

func TestInvalidUTF8TextMessageRejected(t *testing.T) {
	c, tc := newConnPair(t, quietConfig())
	_ = c

	if err := tc.writeFrame(true, OpText, []byte{0xFF, 0xFE}); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseInvalidFramePayloadData {
		t.Fatalf("close code = %d, want %d", code, CloseInvalidFramePayloadData)
	}
}

func TestInvalidUTF8AcrossFragmentsRejected(t *testing.T) {
	_, tc := newConnPair(t, quietConfig())

	// A multi-byte UTF-8 sequence split across fragments must be
	// validated on the reassembled message.
	if err := tc.writeFrame(false, OpText, []byte{0xC3}); err != nil { // start of 'é'
		t.Fatal(err)
	}
	if err := tc.writeFrame(true, OpContinuation, []byte{0x28}); err != nil { // invalid continuation byte
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseInvalidFramePayloadData {
		t.Fatalf("close code = %d, want %d", code, CloseInvalidFramePayloadData)
	}
}

func TestUnexpectedContinuationRejected(t *testing.T) {
	_, tc := newConnPair(t, quietConfig())

	if err := tc.writeFrame(true, OpContinuation, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseProtocolError {
		t.Fatalf("close code = %d, want %d", code, CloseProtocolError)
	}
}

func TestNewMessageBeforeFinalFragmentRejected(t *testing.T) {
	_, tc := newConnPair(t, quietConfig())

	if err := tc.writeFrame(false, OpText, []byte("ab")); err != nil {
		t.Fatal(err)
	}
	if err := tc.writeFrame(true, OpText, []byte("cd")); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseProtocolError {
		t.Fatalf("close code = %d, want %d", code, CloseProtocolError)
	}
}

func TestFragmentedControlFrameRejected(t *testing.T) {
	_, tc := newConnPair(t, quietConfig())

	if err := tc.writeFrame(false, OpPing, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseProtocolError {
		t.Fatalf("close code = %d, want %d", code, CloseProtocolError)
	}
}

func TestInvalidCloseCodeRejected(t *testing.T) {
	_, tc := newConnPair(t, quietConfig())

	// 1006 (0x03EE) is reserved and must never appear on the wire.
	if err := tc.writeFrame(true, OpClose, []byte{0x03, 0xEE}); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseProtocolError {
		t.Fatalf("close code = %d, want %d", code, CloseProtocolError)
	}
}

func TestAbruptTCPClosureReported(t *testing.T) {
	c, tc := newConnPair(t, quietConfig())

	tc.nc.Close() // no close frame: abnormal closure
	_, err := c.ReadMessage()
	var ce *CloseError
	if !errors.As(err, &ce) || ce.Code != CloseAbnormalClosure {
		t.Fatalf("err = %v, want CloseError 1006", err)
	}
}

func TestWriteAfterCloseFails(t *testing.T) {
	c, tc := newConnPair(t, quietConfig())

	if err := tc.writeFrame(true, OpClose, []byte{0x03, 0xE8}); err != nil { // 1000
		t.Fatal(err)
	}
	<-c.Done()
	if err := c.WriteMessage(OpText, []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("WriteMessage = %v, want ErrClosed", err)
	}
}

func TestReadDeadlineUnblocksOnDone(t *testing.T) {
	cfg := quietConfig()
	cfg.CloseTimeout = 100 * time.Millisecond // peer never replies to close
	c, _ := newConnPair(t, cfg)

	done := make(chan struct{})
	go func() {
		c.ReadMessage()
		close(done)
	}()
	go c.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ReadMessage did not unblock after Close")
	}
}
