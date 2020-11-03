package pool

import (
	"context"
	"errors"

	"github.com/renthraysk/xtorm/netx"
)

var ErrPoolClosed = errors.New("pool closed")

type Pool interface {
	netx.Sender
	Close(context.Context) error
}
