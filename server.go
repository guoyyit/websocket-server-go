package websocket

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
	"sync"
)

// errServerClosed is returned by Upgrade when the server has stopped
// accepting new connections.
var errServerClosed = errors.New("websocket: server closed")

// Handler is invoked for each accepted WebSocket connection. It
// typically loops on ReadMessage until it returns an error.
type Handler func(c *Conn)

// Server accepts HTTP requests, upgrades them to WebSocket
// connections and tracks live connections for shutdown.
type Server struct {
	cfg     Config
	handler Handler

	mu     sync.Mutex
	conns  map[*Conn]struct{}
	done   chan struct{}
	closed bool
	once   sync.Once
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
// underlying connection is closed; the same applies to upgrades
// refused because the server is shutting down (HTTP 503).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := s.Upgrade(w, r)
	if err != nil {
		// Upgrade has already written the HTTP error response and
		// closed the underlying connection.
		return
	}
	go c.run()
	defer c.Close()
	s.handler(c)
}

// Upgrade validates the handshake, hijacks the transport, sends 101
// and returns a ready Conn. The caller must run c.run() (or let the
// connection's loops make progress) and eventually close the Conn.
// Every rejection path writes the HTTP error response and then closes
// the underlying TCP connection, so a refused client observes EOF
// right after the response and cannot reuse the connection.
func (s *Server) Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if s.isClosed() {
		rejectUpgrade(w, http.StatusServiceUnavailable, "server shutting down")
		return nil, errServerClosed
	}
	key, err := validateHandshake(r)
	if err != nil {
		rejectUpgrade(w, http.StatusBadRequest, "bad WebSocket handshake")
		return nil, err
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		rejectUpgrade(w, http.StatusInternalServerError, "websocket: server does not support hijacking")
		return nil, errors.New("websocket: hijacking unsupported")
	}
	nc, rw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	c := newConn(nc, rw.Reader, rw.Writer, s.cfg)
	c.onClose = func(*Conn) { s.untrack(c) }
	if !s.track(c) {
		// The server started shutting down after the handshake was
		// validated: refuse the upgrade on the hijacked connection
		// and close it.
		writeErrorResponse(rw.Writer, http.StatusServiceUnavailable, "server shutting down")
		nc.Close()
		return nil, errServerClosed
	}
	if err := writeHandshakeResponse(rw.Writer, computeAcceptKey(key)); err != nil {
		s.untrack(c)
		nc.Close()
		return nil, err
	}
	return c, nil
}

// rejectUpgrade writes an HTTP error response for a refused upgrade
// and closes the underlying TCP connection, so the client observes
// EOF right after the response and the connection cannot be reused.
func rejectUpgrade(w http.ResponseWriter, status int, msg string) {
	if hj, ok := w.(http.Hijacker); ok {
		if nc, rw, err := hj.Hijack(); err == nil {
			writeErrorResponse(rw.Writer, status, msg)
			nc.Close()
			return
		}
	}
	// Non-hijackable writer (e.g. HTTP/2): ask the HTTP server to
	// close the connection after the response.
	w.Header().Set("Connection", "close")
	http.Error(w, msg, status)
}

// writeErrorResponse writes a minimal HTTP error response onto a
// hijacked connection and flushes it. The body matches what
// http.Error would produce.
func writeErrorResponse(w *bufio.Writer, status int, msg string) {
	body := msg + "\n"
	fmt.Fprintf(w, "HTTP/1.1 %d %s\r\nContent-Type: text/plain; charset=utf-8\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\nContent-Length: %d\r\n\r\n%s", status, http.StatusText(status), len(body), body)
	w.Flush()
}

// ConnCount reports the number of live connections.
func (s *Server) ConnCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Close stops accepting new connections and closes every live
// connection, waiting for their close handshakes. Connections are
// closed concurrently: a peer that never answers the close frame is
// reaped after CloseTimeout without delaying the others, so the total
// shutdown time stays bounded by a single close handshake timeout
// regardless of how many peers are silent. Close is idempotent and
// safe for concurrent use; every call waits until all connections
// tracked when it started have been fully torn down.
func (s *Server) Close() error {
	s.once.Do(func() { close(s.done) })
	s.mu.Lock()
	s.closed = true
	conns := make([]*Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
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
	return nil
}

func (s *Server) isClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// track registers c as a live connection. It reports false when the
// server is closing, in which case the caller must refuse the
// upgrade; the check and the registration are atomic with respect to
// Close, so a connection can never slip past a completed shutdown.
func (s *Server) track(c *Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrack(c *Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}
