package fifoch

import (
	"context"
	"sync"

	"github.com/renthraysk/xtorm/netx"
	"github.com/renthraysk/xtorm/netx/pool"
)

// First In, First Out connection pool implemented with a channel.

type poolCh struct {
	mu        sync.Mutex
	ch        chan netx.Conn
	connector netx.Connector
}

// New creates channel based FIFO connection pool.
func New(connector netx.Connector, size int) *poolCh {
	return &poolCh{
		connector: connector,
		ch:        make(chan netx.Conn, size),
	}
}

func (p *poolCh) Close(ctx context.Context) error {

	ch := p.getCh()

	defer close(ch)
	for {
		select {
		case conn, ok := <-ch:
			if !ok {
				return nil
			}
			conn.Close()
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
}

func (p *poolCh) getCh() chan netx.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ch
}

func (p *poolCh) Send(ctx context.Context, wt []byte) ([]netx.Response, error) {

	var (
		conn netx.Conn
		ok   bool
		err  error
	)
	ch := p.getCh()
	select {
	case conn, ok = <-ch:
		if !ok {
			return nil, pool.ErrPoolClosed
		}

	case <-ctx.Done():
		return nil, ctx.Err()

	default:
		if conn, err = p.connector.New(ctx); err != nil {
			return nil, err
		}
	}

	r, err := conn.Send(ctx, wt)
	if err != nil {
		conn.Close()
		return r, err
	}

	ch = p.getCh()
	select {
	case ch <- conn:
	default:
		conn.Close()
	}
	return r, nil
}

var _ pool.Pool = (*poolCh)(nil)
