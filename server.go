package websocket

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// Handler is invoked for each accepted WebSocket connection. It
// typically loops on ReadMessage until it returns an error.
type Handler func(c *Conn)

// Server accepts HTTP requests, upgrades them to WebSocket
// connections and tracks live connections for shutdown.
type Server struct {
	cfg     Config
	handler Handler

	mu    sync.Mutex
	conns map[*Conn]struct{}

	done chan struct{}
	once sync.Once

	// closed guards Upgrade once shutdown has begun. bgWG tracks the
	// in-flight connection loop goroutines so Close can return only
	// after every connection has finished all background activity.
	closed bool
	bgWG   sync.WaitGroup
}

// NewServer creates a Server with the given config and handler.
func NewServer(cfg Config, h Handler) *Server {
	if h == nil {
		panic("websocket: NewServer requires a non-nil handler")
	}
	return &Server{
		cfg:     cfg.withDefaults(),
		handler: h,
		conns:   make(map[*Conn]struct{}),
		done:    make(chan struct{}),
	}
}

// ServeHTTP implements http.Handler. It validates and upgrades the
// request, then runs the connection to completion. Requests that are
// not valid WebSocket upgrades get an HTTP 400 response and the
// underlying connection is closed.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := s.Upgrade(w, r)
	if err != nil {
		// Upgrade has already written the HTTP error response and
		// closed the underlying TCP connection.
		return
	}

	// bgWG was incremented by Upgrade together with connection
	// registration, so a concurrent Close is guaranteed to wait for
	// this goroutine.
	defer c.Close()
	go func() {
		defer s.bgWG.Done()
		c.run()
	}()
	s.handler(c)
}

// Upgrade validates the handshake, hijacks the transport, sends 101
// and returns a ready Conn. The caller must run c.run() (or let the
// connection's loops make progress) and eventually close the Conn. A
// non-nil error means the error response has already been written and
// the underlying TCP connection closed; the request never enters the
// framing phase.
func (s *Server) Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if s.isClosed() {
		s.reject(w, http.StatusServiceUnavailable, "server shutting down")
		return nil, errors.New("websocket: server closed")
	}

	key, err := validateHandshake(r)
	if err != nil {
		s.reject(w, http.StatusBadRequest, "bad WebSocket handshake")
		return nil, err
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		s.reject(w, http.StatusInternalServerError, "websocket: server does not support hijacking")
		return nil, errors.New("websocket: hijacking unsupported")
	}
	nc, rw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	c := newConn(nc, rw.Reader, rw.Writer, s.cfg)
	c.onClose = func(*Conn) { s.untrack(c) }

	// Register the connection (and its run goroutine) and re-check the
	// shutdown flag under the same lock before sending 101. Either the
	// connection is visible to a concurrent Close before the response
	// is written, or it is rejected on the hijacked transport. There is
	// no window in which a connection escapes both paths.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		writeHTTPError(nc, rw.Writer, s.cfg.WriteTimeout, http.StatusServiceUnavailable, "server shutting down")
		return nil, errors.New("websocket: server closed")
	}
	s.conns[c] = struct{}{}
	s.bgWG.Add(1)
	s.mu.Unlock()

	if err := writeHandshakeResponse(rw.Writer, computeAcceptKey(key)); err != nil {
		c.teardown()  // untracks and closes the transport
		s.bgWG.Done() // c.run never starts on this path
		return nil, err
	}
	return c, nil
}

// ConnCount reports the number of live connections.
func (s *Server) ConnCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Close stops accepting new connections and closes every live
// connection, waiting for their close handshakes. Close handshakes
// run concurrently so the total shutdown time is bounded by a single
// connection's CloseTimeout regardless of how many peers stay silent.
// Close is idempotent and safe to call concurrently; only the first
// call initiates shutdown and sends close frames.
func (s *Server) Close() error {
	s.once.Do(func() { close(s.done) })
	s.mu.Lock()
	conns := make([]*Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.closed = true
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Add(1)
		go func(c *Conn) {
			defer wg.Done()
			c.Close()
		}(c)
	}
	wg.Wait()
	// Wait for the connection loop goroutines to finish, so a
	// returning Close guarantees zero residual connection activity.
	s.bgWG.Wait()
	return nil
}

// reject writes an HTTP error response for a failed upgrade and
// guarantees the underlying TCP connection is closed afterwards, so
// the client observes EOF on its next read and cannot reuse the
// connection for another request.
func (s *Server) reject(w http.ResponseWriter, code int, msg string) {
	if hj, ok := w.(http.Hijacker); ok {
		if nc, rw, err := hj.Hijack(); err == nil {
			writeHTTPError(nc, rw.Writer, s.cfg.WriteTimeout, code, msg)
			return
		}
	}
	// Fallback (no hijacker available): ask net/http to close the
	// transport after sending the response.
	w.Header().Set("Connection", "close")
	http.Error(w, msg, code)
}

// writeHTTPError writes an HTTP/1.1 error response directly on a
// hijacked connection, flushes it and closes the transport. The
// format mirrors http.Error. The flush is bounded by writeTimeout so
// a wedged peer cannot delay reclaiming the connection.
func writeHTTPError(nc net.Conn, bw *bufio.Writer, writeTimeout time.Duration, code int, msg string) {
	defer nc.Close()
	if writeTimeout > 0 {
		nc.SetWriteDeadline(time.Now().Add(writeTimeout))
	}
	body := msg + "\n"
	fmt.Fprintf(bw, "HTTP/1.1 %d %s\r\n", code, http.StatusText(code))
	fmt.Fprintf(bw, "Content-Type: text/plain; charset=utf-8\r\n")
	fmt.Fprintf(bw, "X-Content-Type-Options: nosniff\r\n")
	fmt.Fprintf(bw, "Connection: close\r\n")
	fmt.Fprintf(bw, "Content-Length: %d\r\n\r\n", len(body))
	bw.WriteString(body)
	bw.Flush()
}

func (s *Server) isClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *Server) untrack(c *Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}
