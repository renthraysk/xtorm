package xproto

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/renthraysk/xtorm/protobuf/mysqlx"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_crud"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_datatypes"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_expr"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_prepare"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_session"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_sql"
)

func TestAnyLiterals(t *testing.T) {
	const tag = 1

	tests := []interface{}{
		nil,
		true,
		false,
		uint8(0),
		uint8(255),
		uint16(0),
		uint16(math.MaxUint16),
		uint32(0),
		uint32(math.MaxUint32),
		uint64(0),
		uint64(math.MaxUint64),
		uint(0),
		uint(1),
		^uint(0),
		int8(math.MinInt8),
		int8(-1),
		int8(0),
		int8(1),
		int8(math.MaxInt8),
		int16(math.MinInt16),
		int16(-1),
		int16(0),
		int16(1),
		int16(math.MaxInt16),
		int32(math.MinInt32),
		int32(-1),
		int32(0),
		int32(1),
		int32(math.MaxInt32),
		int64(math.MinInt64),
		int64(-1),
		int64(0),
		int64(1),
		int64(math.MaxInt64),
		int(-1),
		int(0),
		int(1),

		float32(0),
		float32(math.MaxFloat32),

		float64(0),
		float64(math.MaxFloat64),
		time.Time{},
		time.Now(),
		time.Duration(-42 * time.Second),
		time.Duration(0),
		time.Duration(42 * time.Second),
		42 * time.Second,
		"",
		"abcdefghijklmnopqrstuvwxyz",
		[]byte{},
		[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%T(%v)", tt, tt), func(t *testing.T) {

			var any mysqlx_datatypes.Any

			b, err := appendAny(nil, tag, tt)

			if err != nil {
				t.Fatalf("failed to marshal any: %s", err)
			}
			if b[0] != tag<<3|proto.WireBytes {
				t.Fatalf("expected WireBytes tag")
			}
			n, nn := binary.Uvarint(b[1:])
			if uint64(len(b)) != 1+uint64(nn)+n {
				t.Fatalf("length incorrect, encoded %d, got %d", 1+uint64(nn)+n, len(b))
			}
			if err := proto.Unmarshal(b[1+nn:], &any); err != nil {
				t.Fatalf("unmarshalling expression failed: %s", err)
			}

			switch v := (tt).(type) {
			case bool:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_BOOL ||
					any.GetScalar().GetVBool() != v {
					t.Fatalf("bool failed, expected %v got %v", v, any.GetScalar().GetVBool())
				}
			case uint8:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_UINT ||
					any.GetScalar().GetVUnsignedInt() != uint64(v) {
					t.Fatalf("unsigned int failed, expected %v got %v", v, any.GetScalar().GetVUnsignedInt())
				}

			case uint16:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_UINT ||
					any.GetScalar().GetVUnsignedInt() != uint64(v) {
					t.Fatalf("unsigned int failed, expected %v got %v", v, any.GetScalar().GetVUnsignedInt())
				}
			case uint32:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_UINT ||
					any.GetScalar().GetVUnsignedInt() != uint64(v) {
					t.Fatalf("unsigned int failed, expected %v got %v", v, any.GetScalar().GetVUnsignedInt())
				}
			case uint64:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_UINT ||
					any.GetScalar().GetVUnsignedInt() != uint64(v) {
					t.Fatalf("unsigned int failed, expected %v got %v", v, any.GetScalar().GetVUnsignedInt())
				}
			case uint:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_UINT ||
					any.GetScalar().GetVUnsignedInt() != uint64(v) {
					t.Fatalf("unsigned int failed, expected %v got %v", v, any.GetScalar().GetVUnsignedInt())
				}
			case int8:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_SINT ||
					any.GetScalar().GetVSignedInt() != int64(v) {
					t.Fatalf("signed int failed, expected %v got %v", v, any.GetScalar().GetVSignedInt())
				}
			case int16:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_SINT ||
					any.GetScalar().GetVSignedInt() != int64(v) {
					t.Fatalf("signed int failed, expected %v got %v", v, any.GetScalar().GetVSignedInt())
				}
			case int32:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_SINT ||
					any.GetScalar().GetVSignedInt() != int64(v) {
					t.Fatalf("signed int failed, expected %v got %v", v, any.GetScalar().GetVSignedInt())
				}
			case int64:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_SINT ||
					any.GetScalar().GetVSignedInt() != int64(v) {
					t.Fatalf("signed int failed, expected %v got %v", v, any.GetScalar().GetVSignedInt())
				}
			case int:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_SINT ||
					any.GetScalar().GetVSignedInt() != int64(v) {
					t.Fatalf("signed int failed, expected %v got %v", v, any.GetScalar().GetVSignedInt())
				}
			case float32:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_FLOAT ||
					any.GetScalar().GetVFloat() != v {
					t.Fatalf("signed int failed, expected %v got %v", v, any.GetScalar().GetVFloat())
				}

			case float64:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_DOUBLE ||
					any.GetScalar().GetVDouble() != v {
					t.Fatalf("double failed, expected %v got %v", v, any.GetScalar().GetVDouble())
				}

			case []byte:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_OCTETS ||
					!bytes.Equal(any.GetScalar().GetVOctets().GetValue(), v) ||
					any.GetScalar().GetVOctets().GetContentType() != 0 {
					t.Fatalf("octets failed, expected %v got %v", v, any.GetScalar().GetVOctets().GetValue())
				}
			case string:
				if any.GetType() != mysqlx_datatypes.Any_SCALAR ||
					any.GetScalar().GetType() != mysqlx_datatypes.Scalar_V_STRING ||
					string(any.GetScalar().GetVString().GetValue()) != v ||
					any.GetScalar().GetVString().GetCollation() != 0 {
					t.Fatalf("string failed, expected %v got %v", v, any.GetScalar().GetVString().GetValue())
				}
			}
		})
	}
}

func TestExprLiterals(t *testing.T) {
	const tag = 1

	tests := []interface{}{
		nil,
		true,
		false,
		uint8(0),
		uint8(255),
		uint16(0),
		uint16(math.MaxUint16),
		uint32(0),
		uint32(math.MaxUint32),
		uint64(0),
		uint64(math.MaxUint64),
		uint(0),
		uint(1),
		^uint(0),
		int8(math.MinInt8),
		int8(-1),
		int8(0),
		int8(1),
		int8(math.MaxInt8),
		int16(math.MinInt16),
		int16(-1),
		int16(0),
		int16(1),
		int16(math.MaxInt16),
		int32(math.MinInt32),
		int32(-1),
		int32(0),
		int32(1),
		int32(math.MaxInt32),
		int64(math.MinInt64),
		int64(-1),
		int64(0),
		int64(1),
		int64(math.MaxInt64),
		int(-1),
		int(0),
		int(1),

		float32(0),
		float32(math.MaxFloat32),

		float64(0),
		float64(math.MaxFloat64),
		time.Time{},
		time.Now(),
		time.Duration(-42 * time.Second),
		time.Duration(0),
		time.Duration(42 * time.Second),
		42 * time.Second,
		"",
		"abcdefghijklmnopqrstuvwxyz",
		[]byte{},
		[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%T(%v)", tt, tt), func(t *testing.T) {

			var expr mysqlx_expr.Expr

			b, err := appendExpr(nil, tag, tt)
			if err != nil {
				t.Fatalf("marshalling failed: %s", err)
			}
			if b[0] != tag<<3|proto.WireBytes {
				t.Fatalf("expected WireBytes tag")
			}
			n, nn := binary.Uvarint(b[1:])
			if uint64(len(b)) != 1+uint64(nn)+n {
				t.Fatalf("length incorrect, encoded %d, got %d", 1+uint64(nn)+n, len(b))
			}
			if err := proto.Unmarshal(b[1+nn:], &expr); err != nil {
				t.Fatalf("unmarshalling expression failed: %s", err)
			}

			switch v := (tt).(type) {
			case bool:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_BOOL ||
					expr.GetLiteral().GetVBool() != v {
					t.Fatalf("bool failed, expected %v got %v", v, expr.GetLiteral().GetVBool())
				}
			case uint8:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_UINT ||
					expr.GetLiteral().GetVUnsignedInt() != uint64(v) {
					t.Fatalf("unsigned int failed, expected %v got %v", v, expr.GetLiteral().GetVUnsignedInt())
				}

			case uint16:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_UINT ||
					expr.GetLiteral().GetVUnsignedInt() != uint64(v) {
					t.Fatalf("unsigned int failed, expected %v got %v", v, expr.GetLiteral().GetVUnsignedInt())
				}
			case uint32:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_UINT ||
					expr.GetLiteral().GetVUnsignedInt() != uint64(v) {
					t.Fatalf("unsigned int failed, expected %v got %v", v, expr.GetLiteral().GetVUnsignedInt())
				}
			case uint64:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_UINT ||
					expr.GetLiteral().GetVUnsignedInt() != uint64(v) {
					t.Fatalf("unsigned int failed, expected %v got %v", v, expr.GetLiteral().GetVUnsignedInt())
				}
			case uint:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_UINT ||
					expr.GetLiteral().GetVUnsignedInt() != uint64(v) {
					t.Fatalf("unsigned int failed, expected %v got %v", v, expr.GetLiteral().GetVUnsignedInt())
				}
			case int8:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_SINT ||
					expr.GetLiteral().GetVSignedInt() != int64(v) {
					t.Fatalf("signed int failed, expected %v got %v", v, expr.GetLiteral().GetVSignedInt())
				}
			case int16:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_SINT ||
					expr.GetLiteral().GetVSignedInt() != int64(v) {
					t.Fatalf("signed int failed, expected %v got %v", v, expr.GetLiteral().GetVSignedInt())
				}
			case int32:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_SINT ||
					expr.GetLiteral().GetVSignedInt() != int64(v) {
					t.Fatalf("signed int failed, expected %v got %v", v, expr.GetLiteral().GetVSignedInt())
				}
			case int64:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_SINT ||
					expr.GetLiteral().GetVSignedInt() != int64(v) {
					t.Fatalf("signed int failed, expected %v got %v", v, expr.GetLiteral().GetVSignedInt())
				}
			case int:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_SINT ||
					expr.GetLiteral().GetVSignedInt() != int64(v) {
					t.Fatalf("signed int failed, expected %v got %v", v, expr.GetLiteral().GetVSignedInt())
				}
			case float32:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_FLOAT ||
					expr.GetLiteral().GetVFloat() != v {
					t.Fatalf("signed int failed, expected %v got %v", v, expr.GetLiteral().GetVFloat())
				}

			case float64:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_DOUBLE ||
					expr.GetLiteral().GetVDouble() != v {
					t.Fatalf("double failed, expected %v got %v", v, expr.GetLiteral().GetVDouble())
				}

			case []byte:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_OCTETS ||
					!bytes.Equal(expr.GetLiteral().GetVOctets().GetValue(), v) ||
					expr.GetLiteral().GetVOctets().GetContentType() != 0 {
					t.Fatalf("octets failed, expected %v got %v", v, expr.GetLiteral().GetVOctets().GetValue())
				}
			case string:
				if expr.GetType() != mysqlx_expr.Expr_LITERAL ||
					expr.GetLiteral().GetType() != mysqlx_datatypes.Scalar_V_STRING ||
					string(expr.GetLiteral().GetVString().GetValue()) != v ||
					expr.GetLiteral().GetVString().GetCollation() != 0 {
					t.Fatalf("string failed, expected %v got %v", v, expr.GetLiteral().GetVString().GetValue())
				}
			}
		})
	}
}

func TestExprColumn(t *testing.T) {
	const tag = 1

	tests := []string{
		"id",
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {

			var expr mysqlx_expr.Expr

			b, err := AppendExprColumn(nil, tag, name)
			if err != nil {
				t.Fatalf("marshalling failed: %s", err)
			}

			if b[0] != tag<<3|proto.WireBytes {
				t.Fatalf("expected WireBytes tag")
			}
			n, nn := binary.Uvarint(b[1:])
			if uint64(len(b)) != 1+uint64(nn)+n {
				t.Fatalf("length incorrect, encoded %d, got %d", 1+uint64(nn)+n, len(b))
			}
			if err := proto.Unmarshal(b[1+nn:], &expr); err != nil {
				t.Fatalf("unmarshalling expression failed: %s", err)
			}

			if expr.GetType() != mysqlx_expr.Expr_IDENT ||
				expr.GetIdentifier().GetName() != name {
				t.Fatalf("name mismatch expected %q, got %q", name, expr.GetIdentifier().GetName())
			}
		})
	}
}

func TestExprOperator(t *testing.T) {

	const tag = 1

	column := AppendExprFunc(func(p []byte, tag uint8) ([]byte, error) {
		return AppendExprColumn(p, tag, "id")
	})

	tests := map[string]struct {
		Name   string
		Params []interface{}
	}{
		"a": {Name: "a"},

		// default
		"default": {Name: "default"},
		// id + 1
		"add": {Name: "+", Params: []interface{}{column, 1}},
		// id = 42
		"eq": {Name: "eq", Params: []interface{}{column, 42}},
		// id IS NOT NULL
		"is_not": {Name: "is_not", Params: []interface{}{column, nil}},
		// id IN (1,2,3,4,5,6,7,8,9)
		"in": {Name: "in", Params: []interface{}{column, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
	}

	for name, expected := range tests {
		t.Run(name, func(t *testing.T) {

			var expr mysqlx_expr.Expr

			b, err := AppendExprOperator(nil, tag, expected.Name, expected.Params)
			if err != nil {
				t.Fatalf("marshalling failed: %s", err)
			}
			if b[0] != tag<<3|proto.WireBytes {
				t.Fatalf("expected WireBytes tag")
			}
			n, nn := binary.Uvarint(b[1:])
			if uint64(len(b)) != 1+uint64(nn)+n {
				t.Fatalf("length incorrect, encoded %d, got %d", 1+uint64(nn)+n, len(b))
			}
			if err := proto.Unmarshal(b[1+nn:], &expr); err != nil {
				t.Fatalf("unmarshalling expression failed: %s", err)
			}

			if expr.GetType() != mysqlx_expr.Expr_OPERATOR {
				t.Fatalf("not operator type, got %s", expr.GetType().String())
			}
			if expr.GetOperator().GetName() != expected.Name {
				t.Fatalf("incorrect name, expected %q, got %q", expected.Name, expr.GetOperator().GetName())
			}

		})
	}
}

func TestExprFunction(t *testing.T) {

	const tag = 1
	tests := map[string]struct {
		Name   string
		Params []interface{}
	}{
		// LAST_INSERT_ID()
		"last_insert_id": {Name: "LAST_INSERT_ID"},
		// ROWCOUNT()
		"rowcount": {Name: "ROW_COUNT"},
	}

	for name, expected := range tests {
		t.Run(name, func(t *testing.T) {

			var expr mysqlx_expr.Expr

			b, err := AppendExprFunctionCall(nil, tag, expected.Name, expected.Params)
			if err != nil {
				t.Fatalf("marshalling failed: %s", err)
			}
			if b[0] != tag<<3|proto.WireBytes {
				t.Fatalf("expected WireBytes tag")
			}
			n, nn := binary.Uvarint(b[1:])
			if uint64(len(b)) != 1+uint64(nn)+n {
				t.Fatalf("length incorrect, encoded %d, got %d", 1+uint64(nn)+n, len(b))
			}
			if err := proto.Unmarshal(b[1+nn:], &expr); err != nil {
				t.Fatalf("unmarshalling expression failed: %s", err)
			}

			if expr.GetType() != mysqlx_expr.Expr_FUNC_CALL {
				t.Fatalf("not func call type, got %s", expr.GetType().String())
			}
			if expr.GetFunctionCall().GetName().GetName() != expected.Name {
				t.Fatalf("incorrect name, expected %q, got %q", expected.Name, expr.GetFunctionCall().GetName().GetName())
			}

		})
	}
}

func TestPrepare(t *testing.T) {

	tests := []struct {
		id   uint32
		stmt string
	}{
		{1, "INSERT INTO foo(a, b) VALUES(?, ?)"},
		{142, "UPDATE foo SET a = ? WHERE b = ?"},
		{math.MaxUint32, "DELETE FROM WHERE id = ?"},
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var p mysqlx_prepare.Prepare
			b := Prepare(nil, tt.id, tt.stmt)
			if int(binary.LittleEndian.Uint32(b)) != len(b)-4 {
				t.Fatalf("incorrect size %d vs %d", len(b)-4, binary.LittleEndian.Uint32(b))
			}
			if b[4] != byte(mysqlx.ClientMessages_PREPARE_PREPARE) {
				t.Fatal("incorrect clientmessage type")
			}
			if err := proto.Unmarshal(b[5:], &p); err != nil {
				t.Fatalf("unmarshal failed: %q", err)
			}
			if p.GetStmtId() != tt.id {
				t.Fatalf("GetStmtId() failed")
			}
			if p.GetStmt().GetType() != mysqlx_prepare.Prepare_OneOfMessage_STMT {
				t.Fatalf("GetStmt().GetType() compared failed")
			}
			if string(p.GetStmt().GetStmtExecute().GetStmt()) != tt.stmt {
				t.Fatalf("GetStmt().GetStmtExecute().GetStmt() compared failed")
			}
		})
	}
}

func TestExecute(t *testing.T) {
	tests := []struct {
		id   uint32
		args []interface{}
	}{
		{1, nil},
		{142, []interface{}{1, 2}},
		{^uint32(0), nil},
		{2, []interface{}{1, "abcdef"}},
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var p mysqlx_prepare.Execute

			b, err := Execute(nil, tt.id, tt.args)
			if err != nil {
				t.Fatalf("failed to marshal execute: %s", err)
			}
			if int(binary.LittleEndian.Uint32(b)) != len(b)-4 {
				t.Fatalf("incorrect size %d vs %d", len(b)-4, binary.LittleEndian.Uint32(b))
			}
			if b[4] != byte(mysqlx.ClientMessages_PREPARE_EXECUTE) {
				t.Fatal("incorrect clientmessage type")
			}
			if err := proto.Unmarshal(b[5:], &p); err != nil {
				t.Fatalf("unmarshal failed: %s", err)
			}
			if p.GetStmtId() != tt.id {
				t.Fatalf("GetStmtId() failed")
			}
		})
	}
}

func TestDeallocate(t *testing.T) {
	tests := []uint32{1, 142, ^uint32(0)}

	for i, id := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var d mysqlx_prepare.Deallocate

			b := Deallocate(nil, id)
			if int(binary.LittleEndian.Uint32(b)) != len(b)-4 {
				t.Fatalf("incorrect size %d vs %d", len(b)-4, binary.LittleEndian.Uint32(b))
			}
			if b[4] != byte(mysqlx.ClientMessages_PREPARE_DEALLOCATE) {
				t.Fatal("incorrect clientmessage type")
			}
			if err := proto.Unmarshal(b[5:], &d); err != nil {
				t.Fatalf("unmarshal failed: %s", err)
			}
			if d.GetStmtId() != id {
				t.Fatalf("GetStmtId() failed")
			}
		})
	}
}

func TestDelete(t *testing.T) {
	tests := []string{"foo", "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"}

	for i, name := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var d mysqlx_crud.Delete

			b, err := Delete(nil, name, nil)
			if err != nil {
				t.Fatalf("failed to marshal delete: %s", err)
			}
			if int(binary.LittleEndian.Uint32(b)) != len(b)-4 {
				t.Fatalf("incorrect size %d vs %d", len(b)-4, binary.LittleEndian.Uint32(b))
			}
			if b[4] != byte(mysqlx.ClientMessages_CRUD_DELETE) {
				t.Fatal("incorrect clientmessage type")
			}
			if err := proto.Unmarshal(b[5:], &d); err != nil {
				t.Fatalf("unmarshal failed: %s", err)
			}
			if d.GetCollection().GetName() != name {
				t.Fatalf("incorrect result from Collection.GetName()")
			}
		})
	}
}

func TestInsert(t *testing.T) {

	tests := []struct {
		name    string
		columns []string
		rows    [][]interface{}
	}{
		{"foo", []string{"id", "col"}, [][]interface{}{
			{0, "zero"},
			{1, "one"},
			{2, "two"},
		}},
	}

	for j, tt := range tests {
		t.Run(strconv.Itoa(j), func(t *testing.T) {
			var i mysqlx_crud.Insert

			b := Insert(nil, tt.name, tt.columns)
			for _, r := range tt.rows {
				var err error
				b, err = AppendInsertRow(b, r)
				if err != nil {
					t.Fatalf("appendInsertRow failed: %s", err)
				}
			}
			/* Next layer up fills in the length
			if int(binary.LittleEndian.Uint32(b)) != len(b)-4 {
				t.Fatalf("incorrect size %d vs %d", len(b)-4, binary.LittleEndian.Uint32(b))
			}
			*/
			if b[4] != byte(mysqlx.ClientMessages_CRUD_INSERT) {
				t.Fatal("incorrect clientmessage type")
			}

			if err := proto.Unmarshal(b[5:], &i); err != nil {
				t.Fatalf("unmarshal failed: %s", err)
			}
			if i.GetCollection().GetName() != tt.name {
				t.Fatal("incorrect result from Collection.GetName()")
			}
			if len(i.GetProjection()) != len(tt.columns) {
				t.Fatalf("incorrect number of columns, expected %d, got %d", len(tt.columns), len(i.GetProjection()))
			}
			for j, name := range tt.columns {
				if i.GetProjection()[j].GetName() != name {
					t.Fatalf("incorrect column name, expected %s, got %s", name, i.GetProjection()[j].GetName())
				}
			}
			if len(i.GetRow()) != len(tt.rows) {
				t.Fatalf("incorrect number of rows, expected %d, got %d", len(tt.rows), len(i.GetRow()))
			}

			// TODO Value checks
		})
	}
}

func TestUpdate(t *testing.T) {

	tests := []struct {
		name     string
		criteria AppendExprFunc
		sets     map[string]interface{}
	}{
		{"foo", nil, map[string]interface{}{"val": "bar"}},

		// UPDATE foo SET val2 = DEFAULT
		{"foo", nil, map[string]interface{}{
			"val2": AppendExprFunc(func(p []byte, tag uint8) ([]byte, error) { return AppendExprOperator(p, tag, "default", nil) })}},

		// UPDATE foo SET val2 = '' WHERE val2 IS NULL
		{"foo", AppendExprFunc(func(p []byte, tag uint8) ([]byte, error) {
			return AppendExprOperator(p, tag, "is", []interface{}{
				AppendExprFunc(func(p []byte, tag uint8) ([]byte, error) {
					return AppendExprColumn(p, tagOperatorParam, "val2")
				}), AppendExprFunc(func(p []byte, tag uint8) ([]byte, error) { return appendExprNull(p, tag), nil })})
		}), map[string]interface{}{"val2": ""}},

		// UPDATE foo SET val = 'foo', val2 = 'id > 2' WHERE id > 2
		{"foo", AppendExprFunc(func(p []byte, tag uint8) ([]byte, error) {
			return AppendExprOperator(p, tag, "==", []interface{}{
				AppendExprFunc(func(p []byte, tag uint8) ([]byte, error) {
					return AppendExprColumn(p, tagOperatorParam, "id")
				}), AppendExprFunc(func(p []byte, tag uint8) ([]byte, error) {
					return appendExprInt64(p, tagOperatorParam, 2), nil
				})})
		}), map[string]interface{}{"val": "foo", "val2": "id > 2"}},
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var u mysqlx_crud.Update

			b, err := Update(nil, tt.name, tt.criteria)
			if err != nil {
				t.Fatalf("update criteria expression: %s", err)
			}
			for k, v := range tt.sets {
				var err error
				b, err = AppendUpdateSet(b, k, v)
				if err != nil {
					t.Fatalf("appendInsertRow failed: %s", err)
				}
			}

			/* Next layer up fills in the length
			if int(binary.LittleEndian.Uint32(b)) != len(b) {
				t.Fatalf("incorrect size %d vs %d", len(b), binary.LittleEndian.Uint32(b))
			}
			*/
			if b[4] != byte(mysqlx.ClientMessages_CRUD_UPDATE) {
				t.Fatal("incorrect clientmessage type")
			}

			//	ioutil.WriteFile("update"+strconv.Itoa(i)+".pb", b[5:], os.ModePerm)

			if err := proto.Unmarshal(b[5:], &u); err != nil {
				t.Fatalf("unmarshal failed: %s", err)
			}
			if u.GetDataModel() != mysqlx_crud.DataModel_TABLE {
				t.Fatal("incorrect result from GetDataModel")
			}
			if u.GetCollection().GetName() != tt.name {
				t.Fatal("incorrect result from Collection.GetName()")
			}
			if len(u.GetOperation()) != len(tt.sets) {
				t.Fatalf("incorrect number of columns, expected %d, got %d", len(tt.sets), len(u.GetOperation()))
			}

			// TODO Value checks
		})
	}
}

func TestReset(t *testing.T) {
	{
		b := Reset(nil, false)
		if b[4] != byte(mysqlx.ClientMessages_SESS_RESET) {
			t.Fatalf("Not session reset")
		}
		var r mysqlx_session.Reset

		if err := proto.Unmarshal(b[5:], &r); err != nil {
			t.Fatalf("failed to unmarshal reset: %s", err)
		}
		if r.GetKeepOpen() {
			t.Fatalf("reset keepopen true")
		}
	}
	{
		b := Reset(nil, true)
		if b[4] != byte(mysqlx.ClientMessages_SESS_RESET) {
			t.Fatalf("Not session reset")
		}
		var r mysqlx_session.Reset
		if err := proto.Unmarshal(b[5:], &r); err != nil {
			t.Fatalf("failed to unmarshal reset: %s", err)
		}
		if !r.GetKeepOpen() {
			t.Fatalf("reset keepopen false")
		}
	}
}

func TestStmtExecute(t *testing.T) {

	tests := []struct {
		stmt string
		args []interface{}
	}{
		{"SELECT 1", nil},
		{"SELECT ?", []interface{}{42}},
		{"SELECT ?", []interface{}{"abcdef"}},
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {

			b, err := StmtExecute(nil, tt.stmt, tt.args)
			if err != nil {
				t.Fatalf("failed to marshal stmtexecute: %s", err)
			}
			if int(binary.LittleEndian.Uint32(b)) != len(b)-4 {
				t.Fatalf("incorrect size %d vs %d", len(b), binary.LittleEndian.Uint32(b))
			}
			if b[4] != byte(mysqlx.ClientMessages_SQL_STMT_EXECUTE) {
				t.Fatal("incorrect clientmessage type")
			}

			var s mysqlx_sql.StmtExecute

			if err := proto.Unmarshal(b[5:], &s); err != nil {
				t.Fatalf("failed to unmarshal stmtexecute: %s", err)
			}
			if string(s.GetStmt()) != tt.stmt {
				t.Fatalf("GetStmt() expected %s, got %s", tt.stmt, s.GetStmt())
			}
		})
	}
}
