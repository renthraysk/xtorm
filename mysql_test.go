// +build mysql

package xtorm

import (
	"database/sql"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func newMySQL(tb testing.TB) *sql.DB {
	cfg := mysql.Config{
		User:                 "usernative",     // Username
		Passwd:               "passwordnative", // Password (requires User)
		Net:                  "tcp",            // Network type
		Addr:                 "127.0.0.1",      // Network address (requires Net)
		DBName:               "gotest",         // Database name
		AllowNativePasswords: true,
//		TLSConfig:            "skip-verify",
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		tb.Fatalf("sql.Open failed: %s", err)
	}
	return db
}

func insertNumbersStmt(tb testing.TB, insertFoo *sql.Stmt) {
	if _, err := insertFoo.Exec(0, "zero"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(1, "one"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(2, "two"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(3, "three"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(4, "four"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(5, "five"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(6, "six"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(7, "seven"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(8, "eight"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(9, "nine"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(10, "ten"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(11, "eleven"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(12, "twelve"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(13, "thirteen"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(14, "fourteen"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(15, "fiveteen"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(16, "sixteen"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(17, "seventeen"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(18, "eighteen"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(19, "nineteen"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
	if _, err := insertFoo.Exec(20, "twenty"); err != nil {
		tb.Fatalf("ExecutePrepared failed: %s", err)
	}
}

func BenchmarkMySqlExecutionPrepared(b *testing.B) {
	db := newMySQL(b)
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatalf("begin failed: %s", err)
		}
		insertFoo, err := tx.Prepare("INSERT INTO foo(id, val) VALUES(?, ?)")
		if err != nil {
			b.Fatalf("prepared failed: %s", err)
		}
		_, err = tx.Exec("DELETE FROM foo")
		if err != nil {
			b.Fatalf("delete failed: %s", err)
		}
		insertNumbersStmt(b, insertFoo)
		insertFoo.Close()
		tx.Commit()
	}
}
