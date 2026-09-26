package websocket

import (
	"bufio"
	"errors"
	"net"
	"testing"
	"time"
)

// newPipePair returns a server Conn backed by an unbuffered net.Pipe,
// so a peer that does not read causes the write loop to block.
func newPipePair(t *testing.T, cfg Config) (*Conn, *testClient) {
	t.Helper()
	serverEnd, clientEnd := net.Pipe()
	c := newConn(serverEnd, bufio.NewReader(serverEnd), bufio.NewWriter(serverEnd), cfg)
	go c.run()
	tc := &testClient{nc: clientEnd, br: bufio.NewReader(clientEnd)}
	t.Cleanup(func() { tc.nc.Close() })
	return c, tc
}

func TestBackpressureWriteBlocksAndObservable(t *testing.T) {
	cfg := quietConfig()
	cfg.WriteBufferSize = 2
	cfg.WriteTimeout = 10 * time.Second
	c, tc := newPipePair(t, cfg)

	// Fill the bounded queue while the peer reads nothing.
	done := make(chan error, 6)
	go func() {
		for i := 0; i < 6; i++ {
			done <- c.WriteMessage(OpText, []byte("x"))
		}
		close(done)
	}()

	// The queue must saturate and stay observable.
	deadline := time.Now().Add(2 * time.Second)
	sawSaturated := false
	for time.Now().Before(deadline) {
		if c.WriteBufferLen() == c.WriteBufferCap() {
			sawSaturated = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !sawSaturated {
		t.Fatalf("write queue never saturated: len=%d cap=%d", c.WriteBufferLen(), c.WriteBufferCap())
	}

	// Draining the peer unblocks the producers.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for i := 0; i < 6; i++ {
			tc.expectMessage(t, OpText, "x")
		}
	}()
	finished := make(chan struct{})
	go func() {
		for err := range done {
			if err != nil {
				t.Errorf("WriteMessage: %v", err)
				return
			}
		}
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("writes remained blocked after the peer drained")
	}
	<-drained
}

func TestSlowWriterDoesNotBlockOtherConnections(t *testing.T) {
	cfg := quietConfig()
	cfg.WriteBufferSize = 2
	cfg.WriteTimeout = 10 * time.Second

	slow, slowTC := newPipePair(t, cfg) // never read until the end
	fast, fastTC := newConnPair(t, cfg)

	// Saturate the slow connection's write pipeline.
	stopped := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			if err := slow.WriteMessage(OpText, []byte("junk")); err != nil {
				return
			}
		}
		close(stopped)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && slow.WriteBufferLen() < slow.WriteBufferCap() {
		time.Sleep(time.Millisecond)
	}
	if slow.WriteBufferLen() != slow.WriteBufferCap() {
		t.Fatalf("slow queue not saturated: %d/%d", slow.WriteBufferLen(), slow.WriteBufferCap())
	}

	// The other connection must make progress unaffected.
	if err := fast.WriteMessage(OpText, []byte("fast")); err != nil {
		t.Fatalf("fast WriteMessage: %v", err)
	}
	fastTC.expectMessage(t, OpText, "fast")
	if err := fast.WriteMessage(OpBinary, []byte("bin")); err != nil {
		t.Fatalf("fast WriteMessage: %v", err)
	}
	fastTC.expectMessage(t, OpBinary, "bin")

	// Drain the slow connection for cleanup.
	go func() {
		for {
			if _, _, err := slowTC.readFrame(); err != nil {
				return
			}
		}
	}()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
	}
}

func TestInboundFrameSizeLimitClosesConn(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxFrameSize = 64
	_, tc := newConnPair(t, cfg)

	if err := tc.writeFrame(true, OpBinary, make([]byte, 65)); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseMessageTooBig {
		t.Fatalf("close code = %d, want %d", code, CloseMessageTooBig)
	}
}

func TestInboundMessageSizeLimitSingleFrame(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxFrameSize = 1 << 20
	cfg.MaxMessageSize = 32
	_, tc := newConnPair(t, cfg)

	if err := tc.writeFrame(true, OpText, make([]byte, 33)); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseMessageTooBig {
		t.Fatalf("close code = %d, want %d", code, CloseMessageTooBig)
	}
}

func TestInboundMessageSizeLimitFragmented(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxMessageSize = 32
	_, tc := newConnPair(t, cfg)

	if err := tc.writeFrame(false, OpBinary, make([]byte, 20)); err != nil {
		t.Fatal(err)
	}
	if err := tc.writeFrame(true, OpContinuation, make([]byte, 20)); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseMessageTooBig {
		t.Fatalf("close code = %d, want %d", code, CloseMessageTooBig)
	}
}

func TestOutboundOversizedMessageRejected(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxMessageSize = 16
	c, _ := newConnPair(t, cfg)

	if err := c.WriteMessage(OpText, make([]byte, 17)); !errors.Is(err, ErrMessageTooBig) {
		t.Fatalf("WriteMessage = %v, want ErrMessageTooBig", err)
	}
}

// autoPongClient reads every server frame, replying to pings.
func autoPongClient(tc *testClient, stop <-chan struct{}) {
	for {
		h, payload, err := tc.readFrame()
		if err != nil {
			return
		}
		if h.op == OpPing {
			_ = tc.writeFrame(true, OpPong, payload)
		}
		select {
		case <-stop:
			return
		default:
		}
	}
}

func TestIdleTimeoutReapsInactiveConn(t *testing.T) {
	cfg := quietConfig()
	cfg.PingInterval = -1
	cfg.IdleTimeout = 80 * time.Millisecond
	cfg.CloseTimeout = 100 * time.Millisecond
	c, tc := newConnPair(t, cfg)

	if code := tc.readClose(t); code != CloseGoingAway {
		t.Fatalf("close code = %d, want %d", code, CloseGoingAway)
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("idle connection not torn down")
	}
}

func TestActivityPreventsIdleReap(t *testing.T) {
	cfg := quietConfig()
	cfg.PingInterval = -1
	cfg.IdleTimeout = 80 * time.Millisecond
	c, tc := newConnPair(t, cfg)

	// Any inbound frame counts as activity.
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(30 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				_ = tc.writeFrame(true, OpPong, nil)
			}
		}
	}()

	time.Sleep(400 * time.Millisecond)
	select {
	case <-c.Done():
		t.Fatal("active connection reaped by idle timeout")
	default:
	}
	close(stop)

	// Connection is still healthy: a real round trip works.
	if err := tc.writeFrame(true, OpText, []byte("alive")); err != nil {
		t.Fatal(err)
	}
	m, err := c.ReadMessage()
	if err != nil || string(m.Data) != "alive" {
		t.Fatalf("post-idle round trip failed: %v", err)
	}
}

func TestHeartbeatPingPongKeepsConnAlive(t *testing.T) {
	cfg := quietConfig()
	cfg.PingInterval = 50 * time.Millisecond
	cfg.PongWait = 200 * time.Millisecond
	cfg.IdleTimeout = -1
	c, tc := newConnPair(t, cfg)

	stop := make(chan struct{})
	go autoPongClient(tc, stop)

	time.Sleep(400 * time.Millisecond)
	select {
	case <-c.Done():
		t.Fatal("connection with healthy pong replies was reaped")
	default:
	}
	close(stop)
}

func TestPongTimeoutReapsDeadConn(t *testing.T) {
	cfg := quietConfig()
	cfg.PingInterval = 50 * time.Millisecond
	cfg.PongWait = 120 * time.Millisecond
	cfg.IdleTimeout = -1
	cfg.CloseTimeout = 100 * time.Millisecond
	c, tc := newConnPair(t, cfg)

	// Read (swallow) pings but never reply.
	go func() {
		for {
			if _, _, err := tc.readFrame(); err != nil {
				return
			}
		}
	}()

	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("dead peer was not reaped by pong timeout")
	}
}
