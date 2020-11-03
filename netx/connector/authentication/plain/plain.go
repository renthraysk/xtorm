package plain

import (
	"github.com/renthraysk/xtorm/netx/connector/authentication"
	"github.com/renthraysk/xtorm/slice"
	"github.com/renthraysk/xtorm/xproto"
)

type auth struct{}

func New() *auth {
	return &auth{}
}

func (auth) Start(buf []byte, c authentication.Credentials) []byte {
	n := len(c.Database()) + 1 + len(c.UserName()) + 1 + len(c.Password())

	buf, ad := slice.Allocate(buf, n)

	i := copy(ad, c.Database())
	ad[i] = 0
	i++
	i += copy(ad[i:], c.UserName())
	ad[i] = 0
	i++
	copy(ad[i:], c.Password())

	return xproto.AuthenticateStart(buf, "PLAIN", ad)
}
