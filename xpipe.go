package xtorm

import (
	"context"

	"github.com/renthraysk/xtorm/netx"
)

// XPipe the top level builder
type XPipe struct {
	Builder
}

func New(n int) *XPipe {
	return &XPipe{Builder{buf: make([]byte, 0, n)}}
}

func (x *XPipe) Send(ctx context.Context, s netx.Sender) ([]netx.Response, error) {
	//	x.xp.Reset(&x.buffer, true) // @TODO MySQL 8.0.16+ specific
	return x.send(ctx, s)
}

func (x *XPipe) Reset() {
	x.reset()
}
