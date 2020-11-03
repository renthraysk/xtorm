package sha256

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/renthraysk/xtorm/netx/connector/authentication"
	"github.com/renthraysk/xtorm/slice"
	"github.com/renthraysk/xtorm/xproto"
)

type auth struct{}

func New() *auth {
	return &auth{}
}

func (auth) Start(buf []byte, c authentication.Credentials) []byte {
	return xproto.AuthenticateStart(buf, "SHA256_MEMORY", nil)
}

func (auth) Continue(buf []byte, c authentication.Credentials, authData []byte) []byte {
	n := len(c.Database()) + 1 + len(c.UserName()) + 1 + 2*sha256.Size

	buf, ad := slice.Allocate(buf, n)

	i := copy(ad, c.Database())
	ad[i] = 0
	i++
	i += copy(ad[i:], c.UserName())
	ad[i] = 0
	i++

	h1 := ad[i : i+sha256.Size]
	h2 := ad[i+sha256.Size:]

	h := sha256.New()
	h.Write([]byte(c.Password()))
	h.Sum(h1[:0])

	h.Reset()
	h.Write(h1)
	h.Sum(h2[:0])

	h.Reset()
	h.Write(h2)
	h.Write(authData)
	h.Sum(h2[:0])

	for i, x := range h1 {
		h2[i] ^= x
	}
	hex.Encode(ad[i:], h2)

	return xproto.AuthenticateContinue(buf, ad)
}
