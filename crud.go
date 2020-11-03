package xtorm

import (
	"encoding/binary"

	"github.com/renthraysk/xtorm/xproto"
)

type Insert struct {
	buf []byte
	err error
}

func (i *Insert) Row(args []interface{}) {
	if i.err == nil {
		i.buf, i.err = xproto.AppendInsertRow(i.buf, args)
	}
}

func (i *Insert) RowV(args ...interface{}) {
	i.Row(args)
}

func (b *Builder) InsertF(name string, columns []string, f func(i *Insert) error) {
	if b.disabled {
		panic("Insert called on non child")
	}
	if b.err != nil {
		return
	}
	n := len(b.buf)
	i := &Insert{buf: xproto.Insert(b.buf, name, columns)}
	b.disabled = true
	f(i)
	b.buf, b.err = i.buf, i.err
	binary.LittleEndian.PutUint32(b.buf[n:], uint32(len(b.buf)-n-4))
	b.disabled = false
}

type Update struct {
	buf []byte
	err error
}

func (u *Update) Set(name string, value interface{}) {
	if u.err == nil {
		u.buf, u.err = xproto.AppendUpdateSet(u.buf, name, value)
	}
}

func (b *Builder) UpdateF(name string, criteria exprFunc, f func(u *Update) error) {
	if b.disabled {
		panic("Update called on non child")
	}
	if b.err != nil {
		return
	}
	n := len(b.buf)
	var u Update
	u.buf, u.err = xproto.Update(b.buf, name, criteria)
	b.disabled = true
	f(&u)
	b.buf, b.err = u.buf, u.err
	binary.LittleEndian.PutUint32(b.buf[n:], uint32(len(b.buf)-n-4))
	b.disabled = false
}
