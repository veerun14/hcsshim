package npipeio

import (
	"context"
	"io"
	"net"
	"sync"
	"syscall"

	winio "github.com/Microsoft/go-winio"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	errorNoData = syscall.Errno(232)
)

type connectionState int

const (
	connectionStateDisconnected connectionState = iota
	connectionStateConnected
	connectionStateAborted
	connectionStateClosed
)

var _ = (io.ReadWriteCloser)(&NamedPipeDelayedConnection{})

// NamedPipeDelayedConnection implements a blocking `io.Reader` `io.Writer`
// `io.Closer`. It blocks until a connection is valid and once valid continues
// the `Read` or `Write` operation. On `Close` unblocks all waiters.
type NamedPipeDelayedConnection struct {
	ctx context.Context

	// l is the named pipe listener to accept connections from
	l net.Listener

	// state is the connection state of `conn`
	state connectionState
	// c is the condition used to wait for a successfuly connection from `l` set
	// to `conn`
	c *sync.Cond
	// conn is the active connection. If a `Read` or `Write` ends in a
	// disconnected state must be set to nil.
	conn net.Conn

	// closer closes `l` and `conn` (if set) and frees all waiters of `c`
	closer sync.Once
	// cerr holds the close error for multiple `Close` call support by contract.
	cerr error
}

// NewNamedPipeDelayedConnection returns a `NamedPipeDelayedConnection` that
// accepts all connections from `pipeListener`.
func NewNamedPipeDelayedConnection(ctx context.Context, pipeListener net.Listener) *NamedPipeDelayedConnection {
	npdc := &NamedPipeDelayedConnection{
		ctx: ctx,
		l:   pipeListener,
		c:   sync.NewCond(new(sync.Mutex)),
	}
	go npdc.reconnect()
	return npdc
}

// Read reads up to len(p) bytes into p. If no connection is available blocks
// until a connection arrives, the connection is aborted, or the connection is
// closed.
func (npdc *NamedPipeDelayedConnection) Read(p []byte) (int, error) {
	npdc.c.L.Lock()
	for npdc.state == connectionStateDisconnected {
		npdc.c.Wait()
	}

	if npdc.state == connectionStateAborted ||
		npdc.state == connectionStateClosed {
		npdc.c.L.Unlock()
		return 0, winio.ErrFileClosed
	}

	n, err := npdc.conn.Read(p)
	if err == io.EOF {
		npdc.state = connectionStateDisconnected
		go npdc.reconnect()
		npdc.c.L.Unlock()
		return npdc.Read(p[n:])
	}
	npdc.c.L.Unlock()
	return n, err
}

// Write writes len(p) bytes from p to the underlying data stream. If no
// connection is available blocks until a connection arrives, the connection is
// aborted, or the connection is closed.
func (npdc *NamedPipeDelayedConnection) Write(p []byte) (int, error) {
	npdc.c.L.Lock()
	for npdc.state == connectionStateDisconnected {
		npdc.c.Wait()
	}

	if npdc.state == connectionStateAborted ||
		npdc.state == connectionStateClosed {
		npdc.c.L.Unlock()
		return 0, winio.ErrFileClosed
	}

	n, err := npdc.conn.Write(p)
	if err == errorNoData {
		npdc.state = connectionStateDisconnected
		go npdc.reconnect()
		npdc.c.L.Unlock()
		return npdc.Write(p[n:])
	}
	npdc.c.L.Unlock()
	return n, err
}

// Close closes the listener for new connections, closes the active connection
// (if any), and frees any waiters.
func (npdc *NamedPipeDelayedConnection) Close() error {
	npdc.closer.Do(func() {
		g, _ := errgroup.WithContext(npdc.ctx)
		g.Go(func() error {
			return npdc.l.Close()
		})
		g.Go(func() error {
			if npdc.conn != nil {
				return npdc.conn.Close()
			}
			return nil
		})
		closeError := g.Wait()

		npdc.c.L.Lock()
		defer npdc.c.L.Unlock()

		if npdc.cerr != nil && closeError != nil {
			npdc.cerr = errors.Wrap(npdc.cerr, closeError.Error())
		} else if closeError != nil {
			npdc.cerr = closeError
		}
		if npdc.state != connectionStateAborted {
			npdc.state = connectionStateClosed
		}
		npdc.c.Broadcast()
	})

	npdc.c.L.Lock()
	defer npdc.c.L.Unlock()
	return npdc.cerr
}

func (npdc *NamedPipeDelayedConnection) reconnect() {
	npdc.c.L.Lock()
	c, err := npdc.l.Accept()
	if err != nil {
		if err == winio.ErrPipeListenerClosed {
			// The listener has been closed. We will never make a connection again.
			npdc.state = connectionStateAborted
		} else {
			// Unexpected error. Close
			npdc.cerr = err
		}
		npdc.c.L.Unlock()
		npdc.Close()
		return
	}
	npdc.state = connectionStateConnected
	npdc.conn = c
	npdc.c.Signal()
	npdc.c.L.Unlock()
}
