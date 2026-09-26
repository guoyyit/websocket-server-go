package websocket

import (
	"testing"
	"time"
)

func TestServerSendsHeartbeatPing(t *testing.T) {
	cfg := quietConfig()
	cfg.PingInterval = 50 * time.Millisecond
	cfg.PongWait = 500 * time.Millisecond
	c, tc := newConnPair(t, cfg)

	// The client answers two heartbeat pings; the connection must
	// stay open while the peer is responsive.
	for i := 0; i < 2; i++ {
		h, payload, err := tc.readFrame()
		if err != nil {
			t.Fatalf("read ping %d: %v", i, err)
		}
		if h.op != OpPing {
			t.Fatalf("frame %d op = %v, want OpPing", i, h.op)
		}
		if err := tc.writeFrame(true, OpPong, payload); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-c.Done():
		t.Fatal("connection closed despite pong replies")
	case <-time.After(150 * time.Millisecond):
	}
}

func TestPongTimeoutReapsDeadPeer(t *testing.T) {
	cfg := quietConfig()
	cfg.PingInterval = 50 * time.Millisecond
	cfg.PongWait = 150 * time.Millisecond
	cfg.CloseTimeout = 100 * time.Millisecond
	c, tc := newConnPair(t, cfg)

	// The client never pongs. The server must eventually send a
	// close frame (going away) and tear the connection down.
	deadline := time.Now().Add(3 * time.Second)
	for {
		h, _, err := tc.readFrame()
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		if h.op == OpClose {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no close frame after pong timeout")
		}
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("connection not torn down after pong timeout")
	}
}

func TestIdleTimeoutReapsInactiveConnection(t *testing.T) {
	cfg := quietConfig()
	cfg.IdleTimeout = 150 * time.Millisecond
	cfg.CloseTimeout = 100 * time.Millisecond
	c, tc := newConnPair(t, cfg)

	if code := tc.readClose(t); code != CloseGoingAway {
		t.Fatalf("close code = %d, want %d (going away)", code, CloseGoingAway)
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("idle connection not torn down")
	}
}

func TestActivityPreventsIdleReap(t *testing.T) {
	cfg := quietConfig()
	cfg.IdleTimeout = 250 * time.Millisecond
	cfg.CloseTimeout = 100 * time.Millisecond
	c, tc := newConnPair(t, cfg)

	// Inbound data every 100ms (well under the 250ms idle limit)
	// must keep the connection alive.
	for i := 0; i < 5; i++ {
		if err := tc.writeFrame(true, OpText, []byte("tick")); err != nil {
			t.Fatal(err)
		}
		if _, err := c.ReadMessage(); err != nil {
			t.Fatalf("ReadMessage %d: %v", i, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	select {
	case <-c.Done():
		t.Fatal("active connection was reaped")
	default:
	}

	// Once traffic stops, the reaper kicks in.
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("connection not reaped after activity stopped")
	}
}

func TestDataActivitySatisfiesPongWait(t *testing.T) {
	cfg := quietConfig()
	cfg.PingInterval = 50 * time.Millisecond
	cfg.PongWait = 200 * time.Millisecond
	cfg.CloseTimeout = 100 * time.Millisecond
	c, tc := newConnPair(t, cfg)

	// The client never pongs but keeps sending data; any inbound
	// frame counts as liveness, so the connection must survive.
	go func() {
		for {
			if _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}()
	for i := 0; i < 6; i++ {
		if err := tc.writeFrame(true, OpText, []byte("data")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(75 * time.Millisecond)
	}
	select {
	case <-c.Done():
		t.Fatal("connection reaped despite steady data traffic")
	default:
	}
}
