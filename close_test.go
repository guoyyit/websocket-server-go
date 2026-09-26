package websocket

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPeerInitiatedCloseHandshake(t *testing.T) {
	c, tc := newConnPair(t, quietConfig())

	// Peer sends close(1000, "bye"); server replies close and reports
	// the CloseError to readers.
	if err := tc.writeFrame(true, OpClose, []byte{0x03, 0xE8, 'b', 'y', 'e'}); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseNormalClosure {
		t.Fatalf("close reply code = %d, want %d", code, CloseNormalClosure)
	}
	_, err := c.ReadMessage()
	var ce *CloseError
	if !errors.As(err, &ce) || ce.Code != CloseNormalClosure || ce.Text != "bye" {
		t.Fatalf("ReadMessage err = %v, want CloseError 1000 bye", err)
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("connection not torn down after close handshake")
	}
}

func TestQueuedDataFlushedBeforeCloseReply(t *testing.T) {
	c, tc := newConnPair(t, quietConfig())

	// Queue application data, then have the peer initiate close.
	// The close reply must come after the queued data.
	if err := c.WriteMessage(OpText, []byte("pending")); err != nil {
		t.Fatal(err)
	}
	if err := tc.writeFrame(true, OpClose, []byte{0x03, 0xE8}); err != nil {
		t.Fatal(err)
	}
	tc.expectMessage(t, OpText, "pending")
	if code := tc.readClose(t); code != CloseNormalClosure {
		t.Fatalf("close code = %d, want %d", code, CloseNormalClosure)
	}
}

func TestServerInitiatedCloseHandshake(t *testing.T) {
	c, tc := newConnPair(t, quietConfig())

	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()

	if code := tc.readClose(t); code != CloseNormalClosure {
		t.Fatalf("close code = %d, want %d", code, CloseNormalClosure)
	}
	if err := tc.writeFrame(true, OpClose, []byte{0x03, 0xE8}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != nil {
			var ce *CloseError
			if !errors.As(err, &ce) || ce.Code != CloseNormalClosure {
				t.Fatalf("Close() = %v", err)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close() did not return after peer replied")
	}
}

func TestCloseTimeoutWithSilentPeer(t *testing.T) {
	cfg := quietConfig()
	cfg.CloseTimeout = 150 * time.Millisecond
	c, tc := newConnPair(t, cfg)
	_ = tc // client never replies to the close frame

	start := time.Now()
	if err := c.Close(); err != nil {
		t.Logf("Close() = %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Close() took %v, CloseTimeout not enforced", d)
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("connection not torn down after Close")
	}
}

func TestConcurrentCloseRace(t *testing.T) {
	// Both sides initiate close at the same time; the handshake must
	// complete exactly once without deadlock or panic.
	for i := 0; i < 20; i++ {
		cfg := quietConfig()
		cfg.CloseTimeout = 200 * time.Millisecond
		c, tc := newConnPair(t, cfg)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); c.Close() }()
		go func() {
			defer wg.Done()
			tc.writeFrame(true, OpClose, []byte{0x03, 0xE8})
			for {
				if _, _, err := tc.readFrame(); err != nil {
					return
				}
			}
		}()
		wg.Wait()
		select {
		case <-c.Done():
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: connection not torn down", i)
		}
		tc.nc.Close()
	}
}

func TestDoubleCloseIsIdempotent(t *testing.T) {
	cfg := quietConfig()
	cfg.CloseTimeout = 100 * time.Millisecond
	c, _ := newConnPair(t, cfg)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Close() }()
	}
	wg.Wait()
	select {
	case <-c.Done():
	default:
		t.Fatal("connection not torn down")
	}
}

func TestEmptyClosePayloadEchoed(t *testing.T) {
	c, tc := newConnPair(t, quietConfig())

	// Close with no status: server replies with an empty close frame
	// and reports CloseNoStatusReceived to the reader.
	if err := tc.writeFrame(true, OpClose, nil); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseNoStatusReceived {
		t.Fatalf("close reply code = %d, want no status", code)
	}
	_, err := c.ReadMessage()
	var ce *CloseError
	if !errors.As(err, &ce) || ce.Code != CloseNoStatusReceived {
		t.Fatalf("ReadMessage err = %v, want CloseError 1005", err)
	}
}

func TestOneByteClosePayloadRejected(t *testing.T) {
	_, tc := newConnPair(t, quietConfig())

	if err := tc.writeFrame(true, OpClose, []byte{0x03}); err != nil {
		t.Fatal(err)
	}
	if code := tc.readClose(t); code != CloseProtocolError {
		t.Fatalf("close code = %d, want %d", code, CloseProtocolError)
	}
}
