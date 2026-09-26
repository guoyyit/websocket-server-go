package websocket

import (
	"bufio"
	"errors"
	"net"
	"testing"
	"time"
)

// newPipeConn returns a server-side Conn over an unbuffered net.Pipe,
// so the peer controls exactly when writes unblock.
func newPipeConn(t *testing.T, cfg Config) (*Conn, net.Conn) {
	t.Helper()
	srvEnd, cliEnd := net.Pipe()
	c := newConn(srvEnd, bufio.NewReader(srvEnd), bufio.NewWriter(srvEnd), cfg)
	go c.run()
	t.Cleanup(func() { cliEnd.Close() })
	return c, cliEnd
}

func TestBackpressureBlocksFullQueue(t *testing.T) {
	cfg := quietConfig()
	cfg.WriteBufferSize = 4
	cfg.WriteTimeout = 200 * time.Millisecond // bounds the stuck pipe flush
	cfg.CloseTimeout = 100 * time.Millisecond
	c, cliEnd := newPipeConn(t, cfg)

	// The peer reads nothing: one frame is in flight in the write
	// loop, four more fill the bounded queue.
	for i := 0; i < 5; i++ {
		if err := c.WriteMessage(OpText, []byte("msg")); err != nil {
			t.Fatalf("WriteMessage %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for c.WriteBufferLen() < 4 {
		if time.Now().After(deadline) {
			t.Fatalf("queue did not fill: len=%d", c.WriteBufferLen())
		}
		time.Sleep(time.Millisecond)
	}

	// The next write must block (backpressure).
	wrote := make(chan error, 1)
	go func() { wrote <- c.WriteMessage(OpText, []byte("blocked")) }()
	select {
	case <-wrote:
		t.Fatal("WriteMessage did not block on a full queue")
	case <-time.After(100 * time.Millisecond):
	}

	// Once the peer reads one frame, the blocked write proceeds.
	if _, _, err := readFrame(bufio.NewReader(cliEnd), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatalf("blocked WriteMessage: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WriteMessage still blocked after peer read")
	}
}

func TestBlockedWriterUnblocksOnClose(t *testing.T) {
	cfg := quietConfig()
	cfg.WriteBufferSize = 1
	cfg.WriteTimeout = 100 * time.Millisecond // bounds the stuck pipe flush
	cfg.CloseTimeout = 100 * time.Millisecond
	c, _ := newPipeConn(t, cfg)

	for i := 0; i < 2; i++ {
		if err := c.WriteMessage(OpText, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	wrote := make(chan error, 1)
	go func() { wrote <- c.WriteMessage(OpText, []byte("stuck")) }()
	time.Sleep(50 * time.Millisecond)

	go c.Close()
	select {
	case err := <-wrote:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked WriteMessage = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked WriteMessage did not unblock on Close")
	}
}

func TestSlowConnectionDoesNotBlockOthers(t *testing.T) {
	// Connection A is congested (peer never reads).
	cfgA := quietConfig()
	cfgA.WriteBufferSize = 2
	cfgA.WriteTimeout = 200 * time.Millisecond // bounds the stuck pipe flush
	cfgA.CloseTimeout = 100 * time.Millisecond
	ca, _ := newPipeConn(t, cfgA)
	for i := 0; i < 3; i++ {
		if err := ca.WriteMessage(OpText, []byte("filler")); err != nil {
			t.Fatal(err)
		}
	}
	blocked := make(chan error, 1)
	go func() { blocked <- ca.WriteMessage(OpText, []byte("stuck")) }()
	time.Sleep(50 * time.Millisecond)

	// Connection B on the same process must be fully responsive.
	cb, tcb := newConnPair(t, quietConfig())
	if err := tcb.writeFrame(true, OpText, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	m, err := cb.ReadMessage()
	if err != nil || string(m.Data) != "ping" {
		t.Fatalf("conn B ReadMessage: %v %q", err, m.Data)
	}
	if err := cb.WriteMessage(OpText, []byte("pong")); err != nil {
		t.Fatal(err)
	}
	tcb.expectMessage(t, OpText, "pong")

	select {
	case <-blocked:
		t.Fatal("conn A write should still be blocked")
	default:
	}
	go ca.Close()
	<-blocked
}

func TestInboundMessageTooBig(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxMessageSize = 10
	cfg.MaxFrameSize = 64
	_, tc := newConnPair(t, cfg)

	if err := tc.writeFrame(true, OpBinary, make([]byte, 20)); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseMessageTooBig {
		t.Fatalf("close code = %d, want %d", code, CloseMessageTooBig)
	}
}

func TestInboundFragmentedMessageTooBig(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxMessageSize = 10
	cfg.MaxFrameSize = 64
	_, tc := newConnPair(t, cfg)

	// Each frame is under the frame limit; the reassembled message
	// exceeds the message limit.
	if err := tc.writeFrame(false, OpBinary, make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if err := tc.writeFrame(true, OpContinuation, make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseMessageTooBig {
		t.Fatalf("close code = %d, want %d", code, CloseMessageTooBig)
	}
}

func TestInboundFrameTooBig(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxFrameSize = 8
	_, tc := newConnPair(t, cfg)

	if err := tc.writeFrame(true, OpBinary, make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseMessageTooBig {
		t.Fatalf("close code = %d, want %d", code, CloseMessageTooBig)
	}
}

func TestOutboundMessageTooBig(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxMessageSize = 10
	c, tc := newConnPair(t, cfg)

	if err := c.WriteMessage(OpBinary, make([]byte, 20)); !errors.Is(err, ErrMessageTooBig) {
		t.Fatalf("WriteMessage = %v, want ErrMessageTooBig", err)
	}
	// The connection must remain usable.
	if err := c.WriteMessage(OpText, []byte("ok")); err != nil {
		t.Fatalf("WriteMessage after rejection: %v", err)
	}
	tc.expectMessage(t, OpText, "ok")
}
