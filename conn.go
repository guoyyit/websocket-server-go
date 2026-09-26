package websocket

import (
	"bufio"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Config tunes a connection or server. A zero value means "use the
// default"; for the duration fields a negative value disables the
// corresponding mechanism (heartbeat, idle reaper).
type Config struct {
	// MaxMessageSize is the maximum reassembled message size in bytes.
	// Larger inbound messages close the connection with
	// CloseMessageTooBig; larger outbound messages are rejected.
	MaxMessageSize int64
	// MaxFrameSize is the maximum single-frame payload in bytes.
	// Outbound messages larger than this are fragmented.
	MaxFrameSize int64
	// WriteBufferSize is the number of outbound frames that may be
	// queued per connection before WriteMessage blocks (backpressure).
	WriteBufferSize int
	// WriteTimeout bounds a single flush to the peer.
	WriteTimeout time.Duration
	// PingInterval is the heartbeat period. Negative disables pings.
	PingInterval time.Duration
	// PongWait is how long after a ping with no inbound activity the
	// connection is considered dead.
	PongWait time.Duration
	// IdleTimeout closes connections with no inbound data for this
	// long. Negative disables the idle reaper.
	IdleTimeout time.Duration
	// CloseTimeout bounds the wait for the peer's close reply after
	// our close frame has been sent.
	CloseTimeout time.Duration
}

// DefaultConfig returns the configuration used for zero values.
func DefaultConfig() Config {
	return Config{
		MaxMessageSize:  1 << 20, // 1 MiB
		MaxFrameSize:    64 << 10,
		WriteBufferSize: 64,
		WriteTimeout:    10 * time.Second,
		PingInterval:    30 * time.Second,
		PongWait:        60 * time.Second,
		IdleTimeout:     5 * time.Minute,
		CloseTimeout:    5 * time.Second,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = d.MaxMessageSize
	}
	if c.MaxFrameSize <= 0 {
		c.MaxFrameSize = d.MaxFrameSize
	}
	if c.WriteBufferSize <= 0 {
		c.WriteBufferSize = d.WriteBufferSize
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = d.WriteTimeout
	}
	if c.PingInterval == 0 {
		c.PingInterval = d.PingInterval
	}
	if c.PongWait == 0 {
		c.PongWait = d.PongWait
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = d.IdleTimeout
	}
	if c.CloseTimeout == 0 {
		c.CloseTimeout = d.CloseTimeout
	}
	return c
}

// Terminal errors returned by ReadMessage/WriteMessage.
var (
	ErrClosed         = errors.New("websocket: connection closed")
	ErrMessageTooBig  = errors.New("websocket: message exceeds maximum size")
	errIdleTimeout    = errors.New("websocket: idle timeout")
	errPongTimeout    = errors.New("websocket: pong timeout")
	errInvalidMessage = errors.New("websocket: WriteMessage requires OpText or OpBinary")
)

// Message is a complete (reassembled) WebSocket message.
type Message struct {
	// Type is OpText or OpBinary.
	Type Opcode
	Data []byte
}

// connState is the connection lifecycle state machine:
// stateOpen -> stateClosing -> stateClosed.
type connState int

const (
	stateOpen connState = iota
	stateClosing
	stateClosed
)

// outbound is one queued unit of work for the write loop.
type outbound struct {
	fin     bool
	op      Opcode
	payload []byte
}

// Conn is a single WebSocket connection. It is safe for concurrent
// use: one goroutine may call ReadMessage while others call
// WriteMessage and Close.
type Conn struct {
	nc  net.Conn
	br  *bufio.Reader
	bw  *bufio.Writer
	cfg Config

	send      chan outbound // bounded data queue (backpressure point)
	ctrl      chan outbound // ping/pong control frames, prioritized
	closeReq  chan outbound // close handshake trigger, capacity 1
	recv      chan Message  // reassembled messages for ReadMessage
	done      chan struct{} // closed on full teardown
	peerClose chan struct{} // closed when the peer's close frame arrives

	writeMu sync.Mutex // serializes fragments of one application message

	mu       sync.Mutex
	state    connState
	closeErr error

	lastActiveNano atomic.Int64

	closeReqOnce sync.Once
	teardownOnce sync.Once
	onClose      func(*Conn)
}

// newConn wraps an established (post-handshake) net.Conn.
func newConn(nc net.Conn, br *bufio.Reader, bw *bufio.Writer, cfg Config) *Conn {
	cfg = cfg.withDefaults()
	c := &Conn{
		nc:        nc,
		br:        br,
		bw:        bw,
		cfg:       cfg,
		send:      make(chan outbound, cfg.WriteBufferSize),
		ctrl:      make(chan outbound, 16),
		closeReq:  make(chan outbound, 1),
		recv:      make(chan Message, 1),
		done:      make(chan struct{}),
		peerClose: make(chan struct{}),
	}
	c.lastActiveNano.Store(time.Now().UnixNano())
	return c
}

// run starts the read, write and watchdog loops and blocks until the
// connection is torn down and all loops have exited.
func (c *Conn) run() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.readLoop() }()
	go func() { defer wg.Done(); c.writeLoop() }()
	if c.cfg.PingInterval > 0 || c.cfg.IdleTimeout > 0 {
		wg.Add(1)
		go func() { defer wg.Done(); c.watchdog() }()
	}
	<-c.done
	wg.Wait()
}

// ReadMessage returns the next complete message. It blocks until a
// message arrives or the connection is closed, in which case it
// returns a non-nil error (a *CloseError if the peer sent close).
func (c *Conn) ReadMessage() (Message, error) {
	// Prefer draining a queued message over reporting closure.
	select {
	case m := <-c.recv:
		return m, nil
	default:
	}
	select {
	case m := <-c.recv:
		return m, nil
	case <-c.done:
		select {
		case m := <-c.recv:
			return m, nil
		default:
		}
		return Message{}, c.err()
	}
}

// WriteMessage queues a complete text or binary message, fragmenting
// it to respect MaxFrameSize. It blocks while the per-connection
// outbound queue is full (backpressure) and returns ErrClosed once
// the connection is closing or closed.
func (c *Conn) WriteMessage(op Opcode, data []byte) error {
	if op != OpText && op != OpBinary {
		return errInvalidMessage
	}
	if int64(len(data)) > c.cfg.MaxMessageSize {
		return ErrMessageTooBig
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.Lock()
	open := c.state == stateOpen
	c.mu.Unlock()
	if !open {
		return ErrClosed
	}

	maxFrame := c.cfg.MaxFrameSize
	first := true
	for {
		fin := true
		chunk := data
		if int64(len(chunk)) > maxFrame {
			chunk = chunk[:maxFrame]
			data = data[len(chunk):]
			fin = false
		}
		frameOp := op
		if !first {
			frameOp = OpContinuation
		}
		select {
		case c.send <- outbound{fin: fin, op: frameOp, payload: chunk}:
		case <-c.done:
			return ErrClosed
		}
		if fin {
			return nil
		}
		first = false
	}
}

// WriteBufferLen reports the current outbound queue depth for
// observability of backpressure.
func (c *Conn) WriteBufferLen() int { return len(c.send) }

// WriteBufferCap reports the outbound queue capacity.
func (c *Conn) WriteBufferCap() int { return cap(c.send) }

// Close initiates the close handshake: it queues a close frame, waits
// for the handshake to complete (bounded by CloseTimeout) and returns
// once the transport is down. It is idempotent and safe to call from
// any goroutine.
func (c *Conn) Close() error {
	c.requestClose(CloseNormalClosure, "")
	<-c.done
	return c.err()
}

// Done returns a channel closed when the connection is torn down.
func (c *Conn) Done() <-chan struct{} { return c.done }

func (c *Conn) err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return ErrClosed
}

// setErr records the terminal error; the first one wins.
func (c *Conn) setErr(err error) {
	c.mu.Lock()
	if c.closeErr == nil {
		c.closeErr = err
	}
	c.mu.Unlock()
}

func (c *Conn) touch() { c.lastActiveNano.Store(time.Now().UnixNano()) }

// requestClose begins the close handshake exactly once; the first
// caller's status code wins.
func (c *Conn) requestClose(code int, text string) {
	c.closeReqOnce.Do(func() {
		c.mu.Lock()
		if c.state == stateOpen {
			c.state = stateClosing
		}
		c.mu.Unlock()
		f := outbound{fin: true, op: OpClose, payload: encodeClosePayload(code, text)}
		select {
		case c.closeReq <- f:
		case <-c.done:
		}
	})
}

// teardown closes the transport and signals completion, exactly once.
// onClose runs before done is closed so that observers of Done()
// (e.g. Server.ConnCount) see the connection fully deregistered.
func (c *Conn) teardown() {
	c.teardownOnce.Do(func() {
		c.mu.Lock()
		c.state = stateClosed
		c.mu.Unlock()
		if c.onClose != nil {
			c.onClose(c)
		}
		close(c.done)
		c.nc.Close()
	})
}

// failClose terminates the connection with a protocol close code.
func (c *Conn) failClose(code int, msg string) {
	c.setErr(&protocolError{code: code, msg: msg})
	c.requestClose(code, msg)
}

// enqueueCtrl queues a control frame without blocking; control frames
// are best-effort when the connection is already congested.
func (c *Conn) enqueueCtrl(op Opcode, payload []byte) bool {
	p := make([]byte, len(payload))
	copy(p, payload)
	select {
	case c.ctrl <- outbound{fin: true, op: op, payload: p}:
		return true
	default:
		return false
	}
}

// readLoop is the frame state machine: it handles control frames
// inline, reassembles fragmented data messages and enforces protocol
// rules, failing the connection with the proper close code.
func (c *Conn) readLoop() {
	var (
		fragmented bool
		msgOp      Opcode
		msgLen     int64
		buf        []byte
	)

	for {
		h, payload, err := readFrame(c.br, c.cfg.MaxFrameSize, true)
		if err != nil {
			if pe, ok := err.(*protocolError); ok {
				c.setErr(pe)
				c.requestClose(pe.code, pe.msg)
			} else {
				// Transport error or EOF. If we are still open this is
				// an abnormal closure (RFC 6455 §7.1.5).
				c.mu.Lock()
				open := c.state == stateOpen
				c.mu.Unlock()
				if open {
					c.setErr(&CloseError{Code: CloseAbnormalClosure, Text: err.Error()})
				}
				c.teardown()
			}
			return
		}
		c.touch()

		switch {
		case h.op == OpPing:
			c.enqueueCtrl(OpPong, payload)
		case h.op == OpPong:
			// touch() above already marks the connection alive.
		case h.op == OpClose:
			ce, perr := parseClosePayload(payload)
			if perr != nil {
				c.setErr(perr)
				c.requestClose(perr.code, perr.msg)
				return
			}
			c.setErr(ce)
			close(c.peerClose)
			c.requestClose(ce.Code, "")
			return
		case h.op == OpContinuation:
			if !fragmented {
				c.failClose(CloseProtocolError, "unexpected continuation frame")
				return
			}
			msgLen += int64(len(payload))
			if msgLen > c.cfg.MaxMessageSize {
				c.failClose(CloseMessageTooBig, "message exceeds maximum size")
				return
			}
			buf = append(buf, payload...)
			if h.fin {
				if !c.deliver(msgOp, buf) {
					return
				}
				fragmented = false
				buf = nil
				msgLen = 0
			}
		default: // OpText or OpBinary starting a message
			if fragmented {
				c.failClose(CloseProtocolError, "new data message before final fragment")
				return
			}
			if h.fin {
				if int64(len(payload)) > c.cfg.MaxMessageSize {
					c.failClose(CloseMessageTooBig, "message exceeds maximum size")
					return
				}
				if !c.deliver(h.op, payload) {
					return
				}
			} else {
				fragmented = true
				msgOp = h.op
				msgLen = int64(len(payload))
				if msgLen > c.cfg.MaxMessageSize {
					c.failClose(CloseMessageTooBig, "message exceeds maximum size")
					return
				}
				buf = payload
			}
		}
	}
}

// deliver validates and queues a complete message for ReadMessage.
// It returns false when the connection is being torn down or the
// message is invalid (in which case the connection is failed).
func (c *Conn) deliver(op Opcode, data []byte) bool {
	if op == OpText && !utf8.Valid(data) {
		c.failClose(CloseInvalidFramePayloadData, "invalid UTF-8 in text message")
		return false
	}
	select {
	case c.recv <- Message{Type: op, Data: data}:
		return true
	case <-c.done:
		return false
	}
}

// writeLoop is the only writer to the wire. It prioritizes control
// frames, applies backpressure through the bounded send queue and
// performs the close handshake.
func (c *Conn) writeLoop() {
	for {
		// Control frames first.
		select {
		case f := <-c.ctrl:
			if !c.writeAndFlush(f) {
				return
			}
			continue
		default:
		}
		select {
		case f := <-c.ctrl:
			if !c.writeAndFlush(f) {
				return
			}
		case f := <-c.send:
			if !c.writeAndFlush(f) {
				return
			}
		case f := <-c.closeReq:
			c.finishClose(f)
			return
		case <-c.done:
			return
		}
	}
}

// writeAndFlush writes one frame and flushes, bounded by WriteTimeout.
func (c *Conn) writeAndFlush(f outbound) bool {
	if err := writeFrame(c.bw, f.fin, f.op, f.payload); err != nil {
		c.setErr(err)
		c.teardown()
		return false
	}
	c.nc.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	if err := c.bw.Flush(); err != nil {
		c.setErr(err)
		c.teardown()
		return false
	}
	return true
}

// finishClose completes the close handshake from the writer side:
// queued application data is flushed first, then the close frame,
// then it waits for the peer's close reply bounded by CloseTimeout.
func (c *Conn) finishClose(f outbound) {
	// Complete unsent data before the close frame.
	draining := true
	for draining {
		select {
		case m := <-c.send:
			if !c.writeAndFlush(m) {
				return
			}
		case p := <-c.ctrl:
			if !c.writeAndFlush(p) {
				return
			}
		default:
			draining = false
		}
	}
	if !c.writeAndFlush(f) {
		return
	}

	// If the peer initiated the close, our reply completes the
	// handshake and the connection can be torn down immediately.
	select {
	case <-c.peerClose:
		c.teardown()
		return
	default:
	}

	// We initiated: wait for the peer's close reply, bounded by
	// CloseTimeout so a silent peer cannot wedge the connection.
	timer := time.NewTimer(c.cfg.CloseTimeout)
	defer timer.Stop()
	select {
	case <-c.peerClose:
	case <-timer.C:
	case <-c.done:
	}
	c.teardown()
}

// watchdog sends heartbeat pings and reaps dead or idle connections.
// Any inbound frame (data or control) counts as activity, so an
// active connection is never reclaimed.
func (c *Conn) watchdog() {
	ping := c.cfg.PingInterval
	idle := c.cfg.IdleTimeout
	pongWait := c.cfg.PongWait
	if pongWait <= 0 {
		pongWait = 2 * ping
	}

	interval := ping
	if interval <= 0 || (idle > 0 && idle < interval) {
		interval = idle
	}
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var pingSentAt time.Time
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			last := time.Unix(0, c.lastActiveNano.Load())

			if idle > 0 && now.Sub(last) > idle {
				c.setErr(errIdleTimeout)
				c.requestClose(CloseGoingAway, "idle timeout")
				return
			}

			if ping <= 0 {
				continue
			}
			if !pingSentAt.IsZero() {
				if !last.Before(pingSentAt) {
					// Activity since the ping: connection is alive.
					pingSentAt = time.Time{}
				} else if now.Sub(pingSentAt) > pongWait {
					c.setErr(errPongTimeout)
					c.requestClose(CloseGoingAway, "pong timeout")
					return
				}
			}
			if pingSentAt.IsZero() {
				c.enqueueCtrl(OpPing, nil)
				pingSentAt = now
			}
		}
	}
}

// parseClosePayload decodes and validates a close frame body
// (RFC 6455 §5.5.1, §7.4.1).
func parseClosePayload(payload []byte) (*CloseError, *protocolError) {
	if len(payload) == 0 {
		return &CloseError{Code: CloseNoStatusReceived}, nil
	}
	if len(payload) == 1 {
		return nil, &protocolError{code: CloseProtocolError, msg: "close payload of 1 byte"}
	}
	code := int(binary.BigEndian.Uint16(payload[:2]))
	text := string(payload[2:])
	if !validCloseCode(code) {
		return nil, &protocolError{code: CloseProtocolError, msg: "invalid close code " + itoa(code)}
	}
	if !utf8.ValidString(text) {
		return nil, &protocolError{code: CloseInvalidFramePayloadData, msg: "invalid UTF-8 in close reason"}
	}
	return &CloseError{Code: code, Text: text}, nil
}

// validCloseCode reports whether code may appear on the wire.
func validCloseCode(code int) bool {
	if code >= 3000 && code <= 4999 {
		return true
	}
	switch code {
	case 1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014:
		return true
	}
	return false
}

// encodeClosePayload builds a close frame body; codes that must not
// appear on the wire (1005/1006) produce an empty body. The reason is
// truncated to fit the 125-byte control frame limit.
func encodeClosePayload(code int, text string) []byte {
	if code == CloseNoStatusReceived || code == CloseAbnormalClosure {
		return nil
	}
	if len(text) > maxControlPayload-2 {
		text = text[:maxControlPayload-2]
		for len(text) > 0 && !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
	}
	p := make([]byte, 2+len(text))
	binary.BigEndian.PutUint16(p, uint16(code))
	copy(p[2:], text)
	return p
}
