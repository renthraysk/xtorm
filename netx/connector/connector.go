package connector

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	"github.com/renthraysk/xtorm/netx"
	"github.com/renthraysk/xtorm/netx/connection"
	"github.com/renthraysk/xtorm/netx/connection/errs"
	"github.com/renthraysk/xtorm/netx/connector/authentication"
	"github.com/renthraysk/xtorm/netx/connector/authentication/mysql41"
	"github.com/renthraysk/xtorm/netx/connector/authentication/plain"
	"github.com/renthraysk/xtorm/xproto"
)

type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type Connector struct {
	network string
	address string

	dialer         Dialer
	tlsConfig      *tls.Config
	authentication authentication.Starter
	userName       string
	password       string
	database       string
}

func New(network, address string, options ...Option) (*Connector, error) {
	cnn := &Connector{
		network:        network,
		address:        address,
		dialer:         new(net.Dialer),
		authentication: mysql41.New(),
	}
	for _, opt := range options {
		if err := opt(cnn); err != nil {
			return nil, err
		}
	}
	return cnn, nil
}

// authentication.Credentials interface implementation
func (c *Connector) UserName() string { return c.userName }
func (c *Connector) Password() string { return c.password }
func (c *Connector) Database() string { return c.database }

// net.Addr interface implementation
func (c *Connector) Network() string { return c.network }
func (c *Connector) String() string  { return c.address }

func (c *Connector) New(ctx context.Context) (netx.Conn, error) {
	netConn, err := c.dialer.DialContext(ctx, c.network, c.address)
	if err != nil {
		return nil, fmt.Errorf("unable to dial new connection: %w", err)
	}

	conn := connection.New(netConn)

	if _, ok := netConn.(*net.TCPConn); ok && c.tlsConfig != nil {
		b, err := xproto.CapabilitySet("tls", true)
		if err != nil {
			return nil, err
		}
		// @TODO response test
		if _, err := conn.Send(ctx, b); err != nil {
			return nil, err
		}
		tlsConn := tls.Client(netConn, c.tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			tlsConn.Close()
			return nil, fmt.Errorf("failed TLS handshake: %w", err)
		}
		conn.Reset(tlsConn)
	}

	if err := conn.Authenticate(ctx, c, c.authentication); err != nil {
		var m *connection.MySqlXError
		if errors.As(err, &m) && m.Code == errs.ErAccessDeniedError && conn.IsSecure() {
			// Connected securely, so can attempt to authenticate with PLAIN,
			// which will populate the cache for caching_sha2 and sha256_password to start working
			if err2 := conn.Authenticate(ctx, c, plain.New()); err2 == nil {
				return conn, nil
			}
		}
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// Option is a functional option for creating the Connector
type Option func(*Connector) error

// WithDatabase sets the database the connector will be default after successful connection and authentication
func WithDatabase(database string) Option {
	return func(cnn *Connector) error {
		cnn.database = database
		return nil
	}
}

// WithAuthentication set the authentication mechanism that will authentication with.
// If authenticating a connection over TLS then either authentication/mysql41 or authentication/sha256.
// If not using a TLS connection then authentication/mysql41 is the only reliable option.
func WithAuthentication(auth authentication.Starter) Option {
	return func(cnn *Connector) error {
		cnn.authentication = auth
		return nil
	}
}

// WithUserPassword set the username and password pair of the account to authenticate with.
func WithUserPassword(userName, password string) Option {
	return func(cnn *Connector) error {
		cnn.userName = userName
		cnn.password = password
		return nil
	}
}

// WithTLSConfig set the TLS configuration to connect to mysqlx with.
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(cnn *Connector) error {
		cnn.tlsConfig = tlsConfig
		return nil
	}
}
