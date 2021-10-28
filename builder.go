package xtorm

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/renthraysk/xtorm/netx"
	"github.com/renthraysk/xtorm/xproto"
)

type IsolationLevel uint

const (
	IsolationLevelDefault IsolationLevel = iota
	IsolationLevelReadUncommitted
	IsolationLevelReadCommitted
	IsolationLevelRepeatableRead
	IsolationLevelSerializable
	IsolationLevelSnapshot
)

type Builder struct {
	buf      []byte
	err      error
	disabled bool
}

func (b *Builder) call(f func(b *Builder) error) {

	var child = Builder{
		buf:      b.buf,
		err:      b.err,
		disabled: false,
	}
	b.disabled = true
	f(&child)
	b.buf = child.buf
	b.err = child.err
	b.disabled = false
}

type OpenContext uint8

const (
	OpenExpectCtxCopyPrev = OpenContext(xproto.OpenExpectCtxCopyPrev)
	OpenExpectCtxEmpty    = OpenContext(xproto.OpenExpectCtxEmpty)
)

func (b *Builder) ExpectFailOnError(context OpenContext, f func(b *Builder) error) {
	if b.disabled {
		panic("Expect called on non child")
	}
	if b.err != nil {
		return
	}
	b.buf = xproto.ExpectOpen(b.buf, xproto.OpenCtxOperation(context), xproto.OpenConditionExpectNoError(true))
	b.call(f)
	b.buf = xproto.ExpectClose(b.buf)
}

// StmtExecute,
func (b *Builder) StmtExecute(stmt string, args ...interface{}) {
	if b.disabled {
		panic("StmtExecute called on non child")
	}
	if b.err != nil {
		return
	}
	b.buf, b.err = xproto.StmtExecute(b.buf, stmt, args)
}

// Prepare, prepare a statement with a given id.
func (b *Builder) Prepare(id uint32, stmt string) {
	if b.disabled {
		panic("Prepare called on non child")
	}
	if b.err != nil {
		return
	}
	b.buf = xproto.Prepare(b.buf, id, stmt)
}

// Execute, executes a prepare statement with given arguments.
func (b *Builder) Execute(id uint32, args ...interface{}) {
	if b.disabled {
		panic("Execute called on non child")
	}
	if b.err != nil {
		return
	}
	b.buf, b.err = xproto.Execute(b.buf, id, args)
}

// Deallocate, deallocate a prepared statement.
func (b *Builder) Deallocate(id uint32) {
	if b.disabled {
		panic("Deallocate called on non child")
	}
	if b.err != nil {
		return
	}
	b.buf = xproto.Deallocate(b.buf, id)
}

// Insert, insert
func (b *Builder) Insert(name string, columns []string, data [][]interface{}) {
	if b.disabled {
		panic("Insert called on non child")
	}
	if b.err != nil {
		return
	}
	n := len(b.buf)

	b.buf = xproto.Insert(b.buf, name, columns)
	for i, row := range data {
		if len(row) != len(columns) {
			b.err = fmt.Errorf("unexpected number of values in row %d, expected %d, got %d", i, len(columns), len(row))
			return
		}
		b.buf, b.err = xproto.AppendInsertRow(b.buf, row)
		if b.err != nil {
			return
		}
	}
	binary.LittleEndian.PutUint32(b.buf[n:], uint32(len(b.buf)-n-4))
}

func (b *Builder) InsertRow(name string, columns []string, row []interface{}) xproto.AppendExprFunc {

	if b.disabled {
		panic("InsertID called on non child")
	}
	if b.err != nil {
		return nil
	}
	n := len(b.buf)
	b.buf = xproto.Insert(b.buf, name, columns)
	b.buf, b.err = xproto.AppendInsertRow(b.buf, row)
	if b.err != nil {
		return nil
	}
	binary.LittleEndian.PutUint32(b.buf[n:], uint32(len(b.buf)-n-4))

	// Generate unique variable name to store the last insert id() in.
	id := "@id$" + strconv.Itoa(len(b.buf))
	b.StmtExecute("SET " + id + " = LAST_INSERT_ID()")
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprVariable(p, tag, id)
	}
}

func (b *Builder) Update(name string, criteria exprFunc, set map[string]interface{}) {
	if b.disabled {
		panic("Update called on non child")
	}
	if b.err != nil {
		return
	}
	n := len(b.buf)

	b.buf, b.err = xproto.Update(b.buf, name, criteria)
	if b.err != nil {
		return
	}
	for k, v := range set {
		b.buf, b.err = xproto.AppendUpdateSet(b.buf, k, v)
		if b.err != nil {
			return
		}
	}
	binary.LittleEndian.PutUint32(b.buf[n:], uint32(len(b.buf)-n-4))
}

// Delete, deletes rows from table named name, and that match the expression criteria.
// Using nil criteria deletes all rows from a table.
func (b *Builder) Delete(name string, criteria exprFunc) {
	if b.disabled {
		panic("Delete called on non child")
	}
	if b.err != nil {
		return
	}
	b.buf, b.err = xproto.Delete(b.buf, name, criteria)
}

// Transaction helper
func (b *Builder) Tx(isolationLevel IsolationLevel, f func(b *Builder) error) {
	if b.disabled {
		panic("Tx called on non child")
	}
	if b.err != nil {
		return
	}
	start := "START TRANSACTION"
	switch isolationLevel {
	case IsolationLevelDefault:
	case IsolationLevelReadUncommitted:
		b.StmtExecute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	case IsolationLevelReadCommitted:
		b.StmtExecute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
	case IsolationLevelRepeatableRead:
		b.StmtExecute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	case IsolationLevelSerializable:
		b.StmtExecute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	case IsolationLevelSnapshot:
		start = "START TRANSACTION WITH CONSISTENT SNAPSHOT"
	default:
		b.err = errors.New("unsupported transaction isolation level")
		return
	}
	b.StmtExecute(start)
	b.call(f)
	b.StmtExecute("COMMIT")
}

func (b *Builder) reset() {
	if b.disabled {
		panic("reset called on non child")
	}
	b.buf = b.buf[:0]
	b.err = nil
}

func (b *Builder) send(ctx context.Context, s netx.Sender) ([]netx.Response, error) {
	if b.disabled {
		panic("send called on non child")
	}
	if b.err != nil {
		return nil, b.err
	}
	return s.Send(ctx, b.buf)
}
