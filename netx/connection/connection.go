package connection

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/renthraysk/xtorm/netx"
	"github.com/renthraysk/xtorm/netx/connector/authentication"
	"github.com/renthraysk/xtorm/protobuf/mysqlx"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_resultset"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_session"

	"github.com/golang/protobuf/proto"
)

const minBufSize = 4096

type conn struct {
	netConn net.Conn
	r       *bufio.Reader
}

func (c *conn) IsSecure() bool {
	switch c.netConn.(type) {
	case *tls.Conn, *net.UnixConn:
		return true
	}
	return false
}

func (c *conn) Reset(netConn net.Conn) {
	c.netConn = netConn
	c.r.Reset(netConn)
}

func New(netConn net.Conn) *conn {
	return &conn{
		netConn: netConn,
		r:       bufio.NewReaderSize(netConn, minBufSize),
	}
}

func (c *conn) Close() error {
	return c.netConn.Close()
}

func (c *conn) WriteOne(ctx context.Context, b []byte) (netx.Response, error) {

	deadline, _ := ctx.Deadline()
	if err := c.netConn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("SetDeadline failed: %w", err)
	}
	if _, err := c.netConn.Write(b); err != nil {
		return nil, fmt.Errorf("Write failed: %w", err)
	}
	return c.Read(ctx, mysqlx.ClientMessages_Type(b[4]))
}

func (c *conn) Send(ctx context.Context, b []byte) ([]netx.Response, error) {
	deadline, _ := ctx.Deadline()
	if err := c.netConn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("SetDeadline failed: %w", err)
	}
	if _, err := c.netConn.Write(b); err != nil {
		return nil, fmt.Errorf("Write failed: %w", err)
	}
	return c.ReadResponsesToSlice(ctx, make([]netx.Response, 0, 16), b)
}

/*
func (c *conn) Send(ctx context.Context, wt *buffer.Buffer) ([]netx.Response, error) {
	deadline, _ := ctx.Deadline()
	if err := c.netConn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("SetDeadline failed: %w", err)
	}
	if _, err := wt.WriteTo(c.netConn); err != nil {
		return nil, fmt.Errorf("WriteTo failed: %w", err)
	}
	r := make([]netx.Response, 0, 16)
	var err error
	for _, b := range *wt {
		r, err = c.ReadResponsesToSlice(ctx, r, b)
		if err != nil {
			return r, err
		}
	}
	return r, nil
}
*/

func (c *conn) ReadResponsesToSlice(ctx context.Context, r []netx.Response, b []byte) ([]netx.Response, error) {
	for i := 0; i < len(b); i += 4 + int(binary.LittleEndian.Uint32(b[i:])) {
		rr, err := c.Read(ctx, mysqlx.ClientMessages_Type(b[i+4]))
		r = append(r, rr)
		if err != nil {
			return r, err
		}
	}
	return r, nil
}

func (c *conn) Read(ctx context.Context, ct mysqlx.ClientMessages_Type) (netx.Response, error) {
	var buf []byte
	var cmd *mysqlx_resultset.ColumnMetaData

	for {
		b, err := c.r.Peek(5)
		if err != nil {
			return nil, err
		}
		u := binary.LittleEndian.Uint32(b)
		st := mysqlx.ServerMessages_Type(b[4])
		c.r.Discard(5) // Peeked 5 bytes so Discard cannot fail.
		n := int(u)
		if n < 1 {
			return nil, fmt.Errorf("length too short, got %d", n)
		}
		n--

		//		log.Printf("<< %s %s(%d)", ct.String(), st.String(), n)

		if n > 0 {
			b, err = c.r.Peek(n)
			if err != nil {
				if err != bufio.ErrBufferFull {
					return nil, err
				}
				if n > cap(buf) {
					buf = make([]byte, n)
				}
				b = buf[:n]
				if _, err := io.ReadFull(c.r, b); err != nil {
					return nil, err
				}
				n = 0 // Read n bytes, no need to Discard()
			}
		}

		switch st {
		case mysqlx.ServerMessages_OK,
			mysqlx.ServerMessages_SESS_AUTHENTICATE_OK,
			mysqlx.ServerMessages_SQL_STMT_EXECUTE_OK:
			c.r.Discard(n)
			return nil, nil

		case mysqlx.ServerMessages_RESULTSET_COLUMN_META_DATA:
			if cmd == nil {
				cmd = new(mysqlx_resultset.ColumnMetaData)
			}
			if err := proto.Unmarshal(b, cmd); err != nil {
				return nil, fmt.Errorf("failed to unmarshal ColumnMetaData: %w", err)
			}

		case mysqlx.ServerMessages_ERROR:
			var er mysqlx.Error

			defer c.r.Discard(n)
			if err := proto.Unmarshal(b, &er); err != nil {
				return nil, fmt.Errorf("failed to unmarshal Error: %w", err)
			}
			e := &MySqlXError{
				Severity: er.GetSeverity(),
				Code:     er.GetCode(),
				SqlState: er.GetSqlState(),
				Msg:      er.GetMsg(),
			}
			if e.IsFatal() {
				return e, e
			}
			switch ct {
			case mysqlx.ClientMessages_SESS_RESET,
				mysqlx.ClientMessages_SESS_AUTHENTICATE_START,
				mysqlx.ClientMessages_SESS_AUTHENTICATE_CONTINUE:
				return e, e
			}
			return e, nil

		case mysqlx.ServerMessages_NOTICE:

		case mysqlx.ServerMessages_SESS_AUTHENTICATE_CONTINUE:
			defer c.r.Discard(n)
			if ct != mysqlx.ClientMessages_SESS_AUTHENTICATE_START {
				return nil, ErrUnexpectedAuthenticateContinue
			}
			var ac mysqlx_session.AuthenticateContinue
			if err := proto.Unmarshal(b, &ac); err != nil {
				return nil, fmt.Errorf("failed to unmarshal AuthenticateContinue: %w", err)
			}
			return nil, &ErrRequireAuthenticateContinue{AuthData: ac.AuthData}

		default:

		}
		c.r.Discard(n)
	}
}

func (c *conn) Authenticate(ctx context.Context, credentials authentication.Credentials, starter authentication.Starter) error {
	var buf [128]byte

	b := starter.Start(buf[:0], credentials)
	_, err := c.WriteOne(ctx, b)
	if err == nil {
		return err
	}
	var ac *ErrRequireAuthenticateContinue
	if !errors.As(err, &ac) {
		return err
	}
	continuer, ok := starter.(authentication.StartContinuer)
	if !ok {
		return ErrUnexpectedAuthenticateContinue
	}
	b = continuer.Continue(buf[:0], credentials, ac.AuthData)
	if _, err = c.WriteOne(ctx, b); err != nil {
		return err
	}
	return nil
}
