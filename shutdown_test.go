package websocket

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"
)

// newHTTPServer starts an httptest.Server and registers its cleanup.
func newHTTPServer(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return hs
}

// waitConnCount polls srv.ConnCount until it reaches want.
func waitConnCount(t *testing.T, srv *Server, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for srv.ConnCount() != want {
		if time.Now().After(deadline) {
			t.Fatalf("ConnCount = %d, want %d", srv.ConnCount(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// silentClient performs a valid upgrade and never reads from or
// replies to the connection.
func silentClient(t *testing.T, url string) *testClient {
	t.Helper()
	return wsDial(t, url)
}

// normalClient upgrades and exchanges one echo round trip.
func normalClient(t *testing.T, url string) *testClient {
	t.Helper()
	tc := wsDial(t, url)
	if err := tc.writeFrame(true, OpText, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	tc.expectMessage(t, OpText, "ping")
	return tc
}

// expectCloseFrame reads one frame and asserts it is a close frame.
func expectCloseFrame(t *testing.T, tc *testClient) {
	t.Helper()
	tc.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	h, _, err := readFrame(tc.br, 1<<20, false)
	if err != nil {
		t.Fatalf("read close frame: %v", err)
	}
	if h.op != OpClose {
		t.Fatalf("got op %v, want close", h.op)
	}
}

// closeReplier waits for the server's close frame, mirrors it and
// drains until EOF. It never calls t.Fatalf, so it is goroutine-safe.
func closeReplier(tc *testClient, errCh chan<- error) {
	tc.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	h, payload, err := readFrame(tc.br, 1<<20, false)
	if err != nil {
		errCh <- err
		return
	}
	if h.op != OpClose {
		errCh <- fmt.Errorf("got op %v, want close", h.op)
		return
	}
	if err := tc.writeFrame(true, OpClose, payload); err != nil {
		errCh <- err
		return
	}
	for {
		tc.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, _, err := tc.readFrame(); err != nil {
			return
		}
	}
}

func shutdownTestConfig() Config {
	cfg := quietConfig()
	cfg.CloseTimeout = 200 * time.Millisecond
	cfg.WriteTimeout = 2 * time.Second
	return cfg
}

func TestShutdownMixedPeersBounded(t *testing.T) {
	cfg := shutdownTestConfig()
	srv := NewServer(cfg, echoHandler)
	hs := newHTTPServer(t, srv)

	const silentCount, normalCount = 6, 4
	silentTCs := make([]*testClient, silentCount)
	for i := range silentTCs {
		silentTCs[i] = silentClient(t, hs.URL)
	}
	normalTCs := make([]*testClient, normalCount)
	replyErrs := make(chan error, normalCount)
	for i := range normalTCs {
		normalTCs[i] = normalClient(t, hs.URL)
	}
	waitConnCount(t, srv, silentCount+normalCount, time.Second)

	// Normal peers reply to the close frame concurrently.
	for _, tc := range normalTCs {
		go closeReplier(tc, replyErrs)
	}

	start := time.Now()
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	d := time.Since(start)

	// Must be close to one CloseTimeout, not N * CloseTimeout.
	if d > 3*cfg.CloseTimeout {
		t.Fatalf("shutdown took %v with %d silent peers, want near single CloseTimeout %v",
			d, silentCount, cfg.CloseTimeout)
	}
	if n := srv.ConnCount(); n != 0 {
		t.Fatalf("ConnCount = %d after shutdown, want 0", n)
	}

	// Silent peers must still observe the close frame.
	for i, tc := range silentTCs {
		expectCloseFrame(t, tc)
		_ = i
	}
	close(replyErrs)
	for err := range replyErrs {
		t.Fatalf("normal peer close reply failed: %v", err)
	}
}

func TestShutdownDurationIndependentOfSilentPeerCount(t *testing.T) {
	cfg := shutdownTestConfig()

	measure := func(n int) time.Duration {
		srv := NewServer(cfg, echoHandler)
		hs := newHTTPServer(t, srv)
		tcs := make([]*testClient, n)
		for i := range tcs {
			tcs[i] = silentClient(t, hs.URL)
		}
		waitConnCount(t, srv, n, time.Second)
		start := time.Now()
		if err := srv.Close(); err != nil {
			t.Fatal(err)
		}
		d := time.Since(start)
		if got := srv.ConnCount(); got != 0 {
			t.Fatalf("ConnCount = %d, want 0", got)
		}
		for i, tc := range tcs {
			tc.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
			h, _, err := readFrame(tc.br, 1<<20, false)
			if err != nil || h.op != OpClose {
				t.Fatalf("n=%d client %d: read close = (op=%v err=%v)", n, i, h.op, err)
			}
			tc.nc.Close()
		}
		return d
	}

	small := measure(2)
	large := measure(30)
	t.Logf("shutdown: 2 silent peers %v; 30 silent peers %v (CloseTimeout %v)",
		small, large, cfg.CloseTimeout)

	// The large case must stay bounded by roughly one CloseTimeout and
	// must not be a multiple of the small case.
	upper := 4 * cfg.CloseTimeout
	if large > upper {
		t.Fatalf("shutdown with 30 silent peers took %v, upper bound %v", large, upper)
	}
	if large > 2*small+200*time.Millisecond {
		t.Fatalf("shutdown scales with peer count: small=%v large=%v", small, large)
	}
}

func TestConcurrentRepeatedShutdown(t *testing.T) {
	cfg := shutdownTestConfig()
	srv := NewServer(cfg, echoHandler)
	hs := newHTTPServer(t, srv)

	const n = 10
	tcs := make([]*testClient, n)
	for i := range tcs {
		tcs[i] = silentClient(t, hs.URL)
	}
	waitConnCount(t, srv, n, time.Second)

	// Count close frames received by each peer.
	closeCount := make([]int, n)
	var countMu sync.Mutex
	var readers sync.WaitGroup
	for i, tc := range tcs {
		readers.Add(1)
		go func(i int, tc *testClient) {
			defer readers.Done()
			count := 0
			for {
				tc.nc.SetReadDeadline(time.Now().Add(time.Second))
				h, _, err := readFrame(tc.br, 1<<20, false)
				if err != nil {
					break
				}
				if h.op == OpClose {
					count++
				}
			}
			countMu.Lock()
			closeCount[i] = count
			countMu.Unlock()
		}(i, tc)
	}

	const callers = 8
	var callersWG sync.WaitGroup
	for i := 0; i < callers; i++ {
		callersWG.Add(1)
		go func() {
			defer callersWG.Done()
			if err := srv.Close(); err != nil {
				t.Errorf("Close() = %v", err)
			}
		}()
	}

	closed := make(chan struct{})
	go func() { callersWG.Wait(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Close calls did not all return")
	}
	if got := srv.ConnCount(); got != 0 {
		t.Fatalf("ConnCount = %d, want 0", got)
	}

	// Repeat shutdown after completion: idempotent, no panic/hang.
	for i := 0; i < 3; i++ {
		if err := srv.Close(); err != nil {
			t.Fatalf("repeat Close() = %v", err)
		}
	}

	// Unblock the reader goroutines by closing the client sockets.
	for _, tc := range tcs {
		tc.nc.Close()
	}
	done := make(chan struct{})
	go func() { readers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("client readers did not exit")
	}

	for i, c := range closeCount {
		if c != 1 {
			t.Fatalf("peer %d received %d close frames, want exactly 1", i, c)
		}
	}
}

func TestShutdownRaceWithNewConnections(t *testing.T) {
	cfg := shutdownTestConfig()
	srv := NewServer(cfg, echoHandler)
	hs := newHTTPServer(t, srv)

	const dialers = 40

	// Accepted upgrades get a close frame; rejected ones get 503 + EOF.
	var acceptedMu sync.Mutex
	var accepted []*testClient
	rejectedCh := make(chan struct{}, dialers)
	errCh := make(chan error, dialers)

	var dialWG sync.WaitGroup
	for i := 0; i < dialers; i++ {
		dialWG.Add(1)
		go func() {
			defer dialWG.Done()
			nc, err := net.Dial("tcp", httpAddr(hs.URL))
			if err != nil {
				return
			}
			if _, err := nc.Write([]byte(validUpgrade)); err != nil {
				nc.Close()
				return
			}
			nc.SetReadDeadline(time.Now().Add(3 * time.Second))
			br := bufio.NewReader(nc)
			resp, err := http.ReadResponse(br,
				&http.Request{Method: http.MethodGet})
			if err != nil {
				nc.Close()
				return
			}
			switch resp.StatusCode {
			case http.StatusSwitchingProtocols:
				resp.Body.Close()
				acceptedMu.Lock()
				accepted = append(accepted, &testClient{
					nc: nc,
					br: br,
				})
				acceptedMu.Unlock()
			case http.StatusServiceUnavailable:
				resp.Body.Close()
				rejectedCh <- struct{}{}
				nc.SetReadDeadline(time.Now().Add(2 * time.Second))
				buf := make([]byte, 16)
				if _, rerr := nc.Read(buf); rerr == nil {
					errCh <- fmt.Errorf("rejected connection not closed")
				} else {
					var ne net.Error
					if errors.As(rerr, &ne) && ne.Timeout() {
						errCh <- fmt.Errorf("rejected connection read timed out, not closed")
					}
				}
				nc.Close()
			default:
				errCh <- fmt.Errorf("unexpected status %d", resp.StatusCode)
			}
		}()
	}

	// Give dialers a head start before shutting down.
	time.Sleep(5 * time.Millisecond)
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	dialWG.Wait()

	if n := srv.ConnCount(); n != 0 {
		t.Fatalf("ConnCount = %d after shutdown race, want 0", n)
	}

	// All accepted connections must have received a close frame.
	acceptedMu.Lock()
	acceptedTCs := append([]*testClient(nil), accepted...)
	acceptedMu.Unlock()
	for i, tc := range acceptedTCs {
		tc.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
		h, _, err := readFrame(tc.br, 1<<20, false)
		if err != nil || h.op != OpClose {
			t.Fatalf("accepted client %d: (op=%v err=%v), want close", i, h.op, err)
		}
		tc.nc.Close()
	}

	// New connections after shutdown returns must get 503 + EOF.
	nc := rawDial(t, httpAddr(hs.URL))
	nc.Write([]byte(validUpgrade))
	resp := readRawResponse(t, nc)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("post-shutdown status = %d, want 503", resp.StatusCode)
	}
	resp.Body.Close()
	expectEOF(t, nc)

	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestShutdownLeavesNoBackgroundGoroutines(t *testing.T) {
	cfg := shutdownTestConfig()
	srv := NewServer(cfg, echoHandler)
	hs := newHTTPServer(t, srv)

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	// A mix of silent and normal peers, larger scale.
	const silentCount, normalCount = 20, 10
	silentTCs := make([]*testClient, silentCount)
	for i := range silentTCs {
		silentTCs[i] = silentClient(t, hs.URL)
	}
	normalTCs := make([]*testClient, normalCount)
	replyErrs := make(chan error, normalCount)
	for i := range normalTCs {
		normalTCs[i] = normalClient(t, hs.URL)
	}
	waitConnCount(t, srv, silentCount+normalCount, time.Second)
	for _, tc := range normalTCs {
		go closeReplier(tc, replyErrs)
	}

	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if n := srv.ConnCount(); n != 0 {
		t.Fatalf("ConnCount = %d, want 0", n)
	}
	close(replyErrs)
	for err := range replyErrs {
		t.Fatalf("normal peer close reply failed: %v", err)
	}

	// Silent peers observe the close frame from the main goroutine.
	for _, tc := range silentTCs {
		expectCloseFrame(t, tc)
	}

	// Release all peers so any draining client-side goroutines and
	// transport goroutines can exit before we measure residue.
	for _, tc := range silentTCs {
		tc.nc.Close()
	}

	// After Close returns, no connection-related goroutines remain.
	runtime.GC()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		runtime.GC()
	}
	if n := runtime.NumGoroutine(); n > baseline+2 {
		t.Fatalf("NumGoroutine = %d after shutdown, baseline %d", n, baseline)
	}
}
