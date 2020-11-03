package netx

import (
	"context"
)

type Response interface {
}

type Connector interface {
	New(ctx context.Context) (Conn, error)
}

type Sender interface {
	Send(ctx context.Context, buffer []byte) ([]Response, error)
}

type Conn interface {
	Sender
	Close() error
	IsSecure() bool
}
