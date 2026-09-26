package websocket

import (
	"errors"
	"net/http"
	"sync"
)

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
// underlying connection is closed.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := s.Upgrade(w, r)
	if err != nil {
		// Upgrade has already written the HTTP error response.
		return
	}
	go c.run()
	defer c.Close()
	s.handler(c)
}

// Upgrade validates the handshake, hijacks the transport, sends 101
// and returns a ready Conn. The caller must run c.run() (or let the
// connection's loops make progress) and eventually close the Conn.
func (s *Server) Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if s.isClosed() {
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return nil, errors.New("websocket: server closed")
	}
	key, err := validateHandshake(r)
	if err != nil {
		http.Error(w, "bad WebSocket handshake", http.StatusBadRequest)
		return nil, err
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket: server does not support hijacking", http.StatusInternalServerError)
		return nil, errors.New("websocket: hijacking unsupported")
	}
	nc, rw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	if err := writeHandshakeResponse(rw.Writer, computeAcceptKey(key)); err != nil {
		nc.Close()
		return nil, err
	}
	c := newConn(nc, rw.Reader, rw.Writer, s.cfg)
	c.onClose = func(*Conn) { s.untrack(c) }
	s.track(c)
	return c, nil
}

// ConnCount reports the number of live connections.
func (s *Server) ConnCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Close stops accepting new connections and closes every live
// connection, waiting for their close handshakes.
func (s *Server) Close() error {
	s.once.Do(func() { close(s.done) })
	s.mu.Lock()
	conns := make([]*Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.closed = true
	s.mu.Unlock()

	for _, c := range conns {
		c.Close()
	}
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

func (s *Server) track(c *Conn) {
	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) untrack(c *Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}
