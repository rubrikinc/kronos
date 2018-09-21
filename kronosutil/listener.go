package kronosutil

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/rubrikinc/kronos/kronosutil/log"
)

// StoppableListener sets TCP keep-alive timeouts on accepted
// connections. It stops listening when stopC is closed.
type StoppableListener struct {
	*net.TCPListener
	stopC <-chan struct{}
}

// NewStoppableListener returns an instance of StoppableListener
func NewStoppableListener(addr string, stopc <-chan struct{}) (*StoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &StoppableListener{ln.(*net.TCPListener), stopc}, nil
}

// Accept listens to and accepts TCP connections
// It returns the connection accepted
func (ln StoppableListener) Accept() (c net.Conn, err error) {
	connC := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connC <- tc
	}()
	select {
	case <-ln.stopC:
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connC:
		ctx := context.TODO()
		if err := tc.SetKeepAlive(true); err != nil {
			log.Error(ctx, err)
		}
		if err := tc.SetKeepAlivePeriod(3 * time.Minute); err != nil {
			log.Error(ctx, err)
		}
		return tc, nil
	}
}
