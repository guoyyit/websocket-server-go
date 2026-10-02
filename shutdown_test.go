package websocket

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// waitConnCount polls until the server tracks exactly want live
// connections.
func waitConnCount(t *testing.T, srv *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for srv.ConnCount() != want {
		if time.Now().After(deadline) {
			t.Fatalf("ConnCount = %d, want %d", srv.ConnCount(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Silent peers must be reaped concurrently: the total shutdown time
// is bounded by a single CloseTimeout no matter how many peers never
// answer the close frame, and responsive peers receive their close
// frames right away.
func TestServerCloseReapsSilentPeersConcurrently(t *testing.T) {
	for _, silent := range []int{1, 8} {
		t.Run(fmt.Sprintf("silent=%d", silent), func(t *testing.T) {
			cfg := testServerConfig()
			cfg.CloseTimeout = 250 * time.Millisecond
			srv := NewServer(cfg, echoHandler)
			hs := httptest.NewServer(srv)
			defer hs.Close()

			const live = 4
			lives := make([]*testClient, 0, live)
			for i := 0; i < silent; i++ {
				wsDial(t, hs.URL) // never replies to the close frame
			}
			for i := 0; i < live; i++ {
				lives = append(lives, wsDial(t, hs.URL))
			}
			waitConnCount(t, srv, silent+live)

			// Responsive peers must see their close frame promptly,
			// without waiting for the silent peers to time out.
			gotClose := make(chan error, live)
			for _, tc := range lives {
				go func(tc *testClient) {
					tc.nc.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
					h, _, err := readFrame(tc.br, 1<<20, false)
					if err != nil {
						gotClose <- err
						return
					}
					if h.op != OpClose {
						gotClose <- fmt.Errorf("op = %v, want OpClose", h.op)
						return
					}
					gotClose <- nil
				}(tc)
			}

			start := time.Now()
			if err := srv.Close(); err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(start)

			// Sequential reaping would take >= silent*CloseTimeout
			// (2s for 8 silent peers); concurrent reaping stays near
			// a single CloseTimeout.
			if elapsed > 1500*time.Millisecond {
				t.Fatalf("Close took %v with %d silent peers, want ~CloseTimeout (%v)", elapsed, silent, cfg.CloseTimeout)
			}
			for i := 0; i < live; i++ {
				if err := <-gotClose; err != nil {
					t.Fatalf("live peer did not receive close frame promptly: %v", err)
				}
			}
			if n := srv.ConnCount(); n != 0 {
				t.Fatalf("ConnCount = %d after Close, want 0", n)
			}
		})
	}
}

// Close must be idempotent and safe for concurrent callers: no panic,
// no hang, and the peer receives exactly one close frame.
func TestServerCloseConcurrentCallers(t *testing.T) {
	cfg := testServerConfig()
	srv := NewServer(cfg, echoHandler)
	hs := httptest.NewServer(srv)
	defer hs.Close()

	wsDial(t, hs.URL) // silent peer, reaped via CloseTimeout
	live := wsDial(t, hs.URL)
	waitConnCount(t, srv, 2)

	const callers = 8
	errs := make([]error, callers)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = srv.Close()
		}(i)
	}
	wg.Wait()
	if d := time.Since(start); d > 1500*time.Millisecond {
		t.Fatalf("concurrent Close took %v, want ~CloseTimeout", d)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Close caller %d = %v", i, err)
		}
	}

	// Repeated calls return promptly.
	start = time.Now()
	for i := 0; i < 3; i++ {
		if err := srv.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("repeated Close took %v, want immediate return", d)
	}
	if n := srv.ConnCount(); n != 0 {
		t.Fatalf("ConnCount = %d after Close, want 0", n)
	}

	// The live peer received exactly one close frame; the next read
	// is EOF, not another close frame.
	if code := live.readClose(t); code != CloseNormalClosure {
		t.Fatalf("close code = %d, want %d", code, CloseNormalClosure)
	}
	live.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := readFrame(live.br, 1<<20, false); err == nil {
		t.Fatal("expected EOF after the single close frame, got another frame")
	}
}

// Upgrades racing with a shutdown must either complete and then be
// closed, or be refused with 503; once Close returns the server must
// track zero connections.
func TestServerCloseRacesWithUpgrade(t *testing.T) {
	for i := 0; i < 25; i++ {
		cfg := testServerConfig()
		srv := NewServer(cfg, echoHandler)
		hs := httptest.NewServer(srv)
		addr := strings.TrimPrefix(hs.URL, "http://")

		stop := make(chan struct{})
		var clients sync.WaitGroup
		for j := 0; j < 4; j++ {
			clients.Add(1)
			go func() {
				defer clients.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					nc, err := net.Dial("tcp", addr)
					if err != nil {
						return
					}
					fmt.Fprintf(nc, "GET /chat HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", addr)
					nc.SetReadDeadline(time.Now().Add(2 * time.Second))
					br := bufio.NewReader(nc)
					resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
					if err != nil {
						nc.Close()
						return
					}
					if resp.StatusCode == http.StatusSwitchingProtocols {
						// The server will close us during shutdown.
						for {
							if _, _, err := readFrame(br, 1<<20, false); err != nil {
								break
							}
						}
					}
					nc.Close()
					if resp.StatusCode == http.StatusServiceUnavailable {
						return
					}
				}
			}()
		}

		time.Sleep(5 * time.Millisecond) // let some upgrades land
		if err := srv.Close(); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		close(stop)

		done := make(chan struct{})
		go func() { clients.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: racing clients stuck", i)
		}
		if n := srv.ConnCount(); n != 0 {
			t.Fatalf("iteration %d: ConnCount = %d after Close, want 0", i, n)
		}
		hs.Close()
	}
}

// After Close returns, every connection must be fully torn down and
// no connection-related background activity may remain.
func TestServerShutdownLeavesNoBackgroundActivity(t *testing.T) {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	cfg := testServerConfig()
	srv := NewServer(cfg, echoHandler)
	hs := httptest.NewServer(srv)

	clients := make([]*testClient, 0, 6)
	for i := 0; i < 6; i++ {
		clients = append(clients, wsDial(t, hs.URL))
	}
	waitConnCount(t, srv, 6)

	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if n := srv.ConnCount(); n != 0 {
		t.Fatalf("ConnCount = %d after Close, want 0", n)
	}
	hs.Close()
	for _, tc := range clients {
		tc.nc.Close()
	}
	waitGoroutineCount(t, baseline+4)
}
