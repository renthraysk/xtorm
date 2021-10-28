package xtorm

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/renthraysk/xtorm/netx"
	"github.com/renthraysk/xtorm/netx/connector"
)

const (
	InsertFoo uint32 = 1 + iota
)

const (
	ipAddress   = "127.0.0.1:33060"
	sockAddress = "/var/run/mysqld/mysqlx.sock"

	bufferSize = 1024
)

func NewConnector(tb testing.TB) netx.Connector {

	tb.Helper()
	connect, err := connector.New("tcp", ipAddress,
		connector.WithUserPassword("usernative", "passwordnative"),
		connector.WithDatabase("gotest"),
	//	connector.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}),
	)
	if err != nil {
		tb.Fatalf("creating connector failed: %q", err)
	}
	return connect
}

func NewConn(tb testing.TB) netx.Conn {
	tb.Helper()
	connect := NewConnector(tb)
	conn, err := connect.New(context.Background())
	if err != nil {
		tb.Fatalf("creating connection failed: %q", err)
	}
	return conn
}

func insertNumbersPrepared(b *Builder) {
	b.Execute(InsertFoo, 0, "zero")
	b.Execute(InsertFoo, 1, "one")
	b.Execute(InsertFoo, 2, "two")
	b.Execute(InsertFoo, 3, "three")
	b.Execute(InsertFoo, 4, "four")
	b.Execute(InsertFoo, 5, "five")
	b.Execute(InsertFoo, 6, "six")
	b.Execute(InsertFoo, 7, "seven")
	b.Execute(InsertFoo, 8, "eight")
	b.Execute(InsertFoo, 9, "nine")
	b.Execute(InsertFoo, 10, "ten")
	b.Execute(InsertFoo, 11, "eleven")
	b.Execute(InsertFoo, 12, "twelve")
	b.Execute(InsertFoo, 13, "thirteen")
	b.Execute(InsertFoo, 14, "fourteen")
	b.Execute(InsertFoo, 15, "fiveteen")
	b.Execute(InsertFoo, 16, "sixteen")
	b.Execute(InsertFoo, 17, "seventeen")
	b.Execute(InsertFoo, 18, "eighteen")
	b.Execute(InsertFoo, 19, "nineteen")
	b.Execute(InsertFoo, 20, "twenty")
}

func insertNumbersMultiRow(b *Builder) {
	b.InsertF("foo", []string{"id", "val"}, func(i *Insert) error {
		i.RowV(0, "zero")
		i.RowV(1, "one")
		i.RowV(2, "two")
		i.RowV(3, "three")
		i.RowV(4, "four")
		i.RowV(5, "five")
		i.RowV(6, "six")
		i.RowV(7, "seven")
		i.RowV(8, "eight")
		i.RowV(9, "nine")
		i.RowV(10, "ten")
		i.RowV(11, "eleven")
		i.RowV(12, "twelve")
		i.RowV(13, "thirteen")
		i.RowV(14, "fourteen")
		i.RowV(15, "fifteen")
		i.RowV(16, "sixteen")
		i.RowV(17, "seventeen")
		i.RowV(18, "eighteen")
		i.RowV(19, "nineteen")
		i.RowV(20, "twenty")
		return nil
	})
}

func TestInsertPrepared(t *testing.T) {
	x := New(bufferSize)
	x.ExpectFailOnError(OpenExpectCtxEmpty, func(b *Builder) error {
		b.Prepare(InsertFoo, "INSERT INTO foo(id, val) VALUES(?, ?)")
		b.Tx(IsolationLevelDefault, func(b *Builder) error {
			b.Delete("foo", nil)
			insertNumbersPrepared(b)
			return nil
		})
		return nil
	})

	conn := NewConn(t)
	defer conn.Close()
	r, err := x.Send(context.Background(), conn)
	if err != nil {
		t.Fatalf("send failed: %q", err)
	}
	fmt.Printf("%+v\n", r)
}

func TestInsertMultirow(t *testing.T) {
	x := New(bufferSize)
	x.ExpectFailOnError(OpenExpectCtxEmpty, func(b *Builder) error {
		b.Prepare(InsertFoo, "INSERT INTO foo(id, val) VALUES(?, ?)")
		b.Tx(IsolationLevelDefault, func(b *Builder) error {
			b.Delete("foo", nil)
			insertNumbersMultiRow(b)
			return nil
		})
		return nil
	})

	conn := NewConn(t)
	defer conn.Close()
	r, err := x.Send(context.Background(), conn)
	if err != nil {
		t.Fatalf("send failed: %q", err)
	}
	fmt.Printf("%+v\n", r)
}

func TestUpdateExecution(t *testing.T) {
	x := New(bufferSize)
	x.ExpectFailOnError(OpenExpectCtxEmpty, func(b *Builder) error {
		b.UpdateF("foo", Eq(Column("id"), 1), func(u *Update) error {
			u.Set("val2", DateAdd(time.Now(), 100, UnitHour))
			return nil
		})
		return nil
	})

	conn := NewConn(t)
	defer conn.Close()
	r, err := x.Send(context.Background(), conn)
	if err != nil {
		t.Fatalf("send failed: %q", err)
	}
	fmt.Printf("%+v\n", r)
}

func TestLastInsertID(t *testing.T) {
	//ERROR 5153 (HY000): Mysqlx::Expr::Expr::VARIABLE is not supported yet
	t.Skip("Mysqlx::Expr::Expr::VARIABLE is currently unsupported in MySQL's X plugin")

	x := New(bufferSize)

	x.ExpectFailOnError(OpenExpectCtxEmpty, func(b *Builder) error {
		b.Tx(IsolationLevelDefault, func(b *Builder) error {
			// Insert row capturing the LAST_INSERT_ID()
			xmlID := b.InsertRow("xml", []string{"xml"}, []interface{}{XMLString("<test></test>")})
			// Insert child row, using the LAST_INSERT_ID() from above.
			b.InsertRow("xmlchild", []string{"xmlid", "value"}, []interface{}{xmlID, "xml fk"})
			return nil
		})
		return nil
	})
	conn := NewConn(t)
	defer conn.Close()
	r, err := x.Send(context.Background(), conn)
	if err != nil {
		t.Fatalf("send failed: %q", err)
	}
	fmt.Printf("%+v\n", r)
}

func BenchmarkGenerationPreparedNew(tb *testing.B) {
	tb.ReportAllocs()
	for i := 0; i < tb.N; i++ {
		x := New(bufferSize)
		x.ExpectFailOnError(OpenExpectCtxEmpty, func(b *Builder) error {
			b.Prepare(InsertFoo, "INSERT INTO foo(id, val) VALUES(?, ?)")
			b.Tx(IsolationLevelDefault, func(b *Builder) error {
				b.Delete("foo", nil)
				insertNumbersPrepared(b)
				return nil
			})
			return nil
		})
	}
}

func BenchmarkGenerationPreparedReused(tb *testing.B) {
	x := New(bufferSize)
	tb.ReportAllocs()
	for i := 0; i < tb.N; i++ {
		x.ExpectFailOnError(OpenExpectCtxEmpty, func(b *Builder) error {
			b.Prepare(InsertFoo, "INSERT INTO foo(id, val) VALUES(?, ?)")
			b.Tx(IsolationLevelDefault, func(b *Builder) error {
				b.Delete("foo", nil)
				insertNumbersPrepared(b)
				return nil
			})
			return nil
		})
		x.Reset()
	}
}

func BenchmarkGenerationMultiRowNew(tb *testing.B) {

	tb.ReportAllocs()
	for i := 0; i < tb.N; i++ {
		x := New(bufferSize)
		x.ExpectFailOnError(OpenExpectCtxEmpty, func(b *Builder) error {
			b.Tx(IsolationLevelDefault, func(b *Builder) error {
				b.Delete("foo", nil)
				insertNumbersMultiRow(b)
				return nil
			})
			return nil
		})
	}
}

func BenchmarkGenerationMultiRowReused(tb *testing.B) {
	x := New(bufferSize)
	tb.ReportAllocs()
	for i := 0; i < tb.N; i++ {
		x.ExpectFailOnError(OpenExpectCtxEmpty, func(b *Builder) error {
			b.Tx(IsolationLevelDefault, func(b *Builder) error {
				b.Delete("foo", nil)
				insertNumbersMultiRow(b)
				return nil
			})
			return nil
		})
		x.Reset()
	}
}

func BenchmarkExecutionPreparedNew(b *testing.B) {
	conn := NewConn(b)
	defer conn.Close()

	b.ResetTimer()
	b.ReportAllocs()

	x := New(bufferSize)
	for i := 0; i < b.N; i++ {
		x.ExpectFailOnError(OpenExpectCtxEmpty, func(b *Builder) error {
			b.Prepare(InsertFoo, "INSERT INTO foo(id, val) VALUES(?, ?)")
			b.Tx(IsolationLevelDefault, func(b *Builder) error {
				b.Delete("foo", nil)
				insertNumbersPrepared(b)
				return nil
			})
			return nil
		})

		if r, err := x.Send(context.Background(), conn); err != nil {
			b.Fatalf("Send failed: %s %+v", err, r)
		}
		x.Reset()
	}
}

func BenchmarkExecutionPreparedReused(b *testing.B) {
	conn := NewConn(b)
	defer conn.Close()

	b.ResetTimer()
	b.ReportAllocs()

	x := New(bufferSize)

	for i := 0; i < b.N; i++ {
		x.ExpectFailOnError(OpenExpectCtxEmpty, func(b *Builder) error {
			b.Prepare(InsertFoo, "INSERT INTO foo(id, val) VALUES(?, ?)")
			b.Tx(IsolationLevelDefault, func(b *Builder) error {
				b.Delete("foo", nil)
				insertNumbersPrepared(b)
				return nil
			})
			return nil
		})
		if r, err := x.Send(context.Background(), conn); err != nil {
			b.Fatalf("Send failed: %s %+v", err, r)
		}
		x.Reset()
	}
}

func BenchmarkExecutionMultiRowReused(tb *testing.B) {
	conn := NewConn(tb)
	defer conn.Close()

	tb.ResetTimer()
	tb.ReportAllocs()

	x := New(bufferSize)
	for i := 0; i < tb.N; i++ {
		x.ExpectFailOnError(OpenExpectCtxEmpty, func(b *Builder) error {
			b.Tx(IsolationLevelDefault, func(b *Builder) error {
				b.Delete("foo", nil)
				insertNumbersMultiRow(b)
				return nil
			})
			return nil
		})
		if r, err := x.Send(context.Background(), conn); err != nil {
			tb.Fatalf("Send failed: %s %+v", err, r)
		}
		x.Reset()
	}
}
