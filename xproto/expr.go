package xproto

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"time"

	"github.com/renthraysk/xtorm/collation"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_datatypes"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_expr"
	"github.com/renthraysk/xtorm/slice"
)

const (
	tagExprType         = 1
	tagExprIdentifier   = 2
	tagExprVariable     = 3
	tagExprLiteral      = 4
	tagExprFunctionCall = 5
	tagExprOperator     = 6
	tagExprPosition     = 7
	tagExprObject       = 8
	tagExprArray        = 9

	// Tag from Operator protobuf
	tagOperatorName  = 1
	tagOperatorParam = 2

	// Tags from FunctionCall protobuf
	tagFunctionCallName  = 1
	tagFunctionCallParam = 2

	// Tag from ColumnIdentifier protobuf
	tagColumnIdentifierDocumentPath = 1
	tagColumnIdentifierName         = 2
	tagColumnIndetifierTable        = 3
	tagColumnIdentifierSchema       = 4
)

func appendExprNull(p []byte, tag uint8) []byte {
	return append(p, tag<<3|wireBytes, 6,
		tagExprType<<3|wireVarint, byte(mysqlx_expr.Expr_LITERAL),
		tagExprLiteral<<3|wireBytes, 2,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_NULL))
}

func appendExprBool(p []byte, tag uint8, x bool) []byte {
	return append(p, tag<<3|wireBytes, 8,
		tagExprType<<3|wireVarint, byte(mysqlx_expr.Expr_LITERAL),
		tagExprLiteral<<3|wireBytes, 4,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_BOOL),
		tagScalarBool<<3|wireVarint, encodeBool(x))
}

func appendExprUint64(p []byte, tag uint8, x uint64) []byte {
	n := int(9*uint32(bits.Len64(x))+64) / 64
	p = append(p, tag<<3|wireBytes, 7+byte(n),
		tagExprType<<3|wireVarint, byte(mysqlx_expr.Expr_LITERAL),
		tagExprLiteral<<3|wireBytes, 3+byte(n),
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_UINT),
		tagScalarUint<<3|wireVarint, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14)|0x80, byte(x>>21)|0x80, byte(x>>28)|0x80,
		byte(x>>35)|0x80, byte(x>>42)|0x80, byte(x>>49)|0x80, byte(x>>56), 1)
	n += len(p) - binary.MaxVarintLen64
	p[n-1] &= 0x7F
	return p[:n]
}

func appendExprInt64(p []byte, tag uint8, v int64) []byte {
	x := uint64(v)<<1 ^ uint64(v>>63)
	n := int(9*uint32(bits.Len64(x))+64) / 64
	p = append(p, tag<<3|wireBytes, 7+byte(n),
		tagExprType<<3|wireVarint, byte(mysqlx_expr.Expr_LITERAL),
		tagExprLiteral<<3|wireBytes, 3+byte(n),
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_SINT),
		tagScalarSint<<3|wireVarint, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14)|0x80, byte(x>>21)|0x80, byte(x>>28)|0x80,
		byte(x>>35)|0x80, byte(x>>42)|0x80, byte(x>>49)|0x80, byte(x>>56), 1)
	n += len(p) - binary.MaxVarintLen64
	p[n-1] &= 0x7F
	return p[:n]
}

func appendExprFloat32(p []byte, tag uint8, f float32) []byte {
	x := math.Float32bits(f)
	return append(p, tag<<3|wireBytes, 11,
		tagExprType<<3|wireVarint, byte(mysqlx_expr.Expr_LITERAL),
		tagExprLiteral<<3|wireBytes, 7,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_FLOAT),
		tagScalarFloat<<3|wireFixed32, byte(x), byte(x>>8), byte(x>>16), byte(x>>24))
}

func appendExprFloat64(p []byte, tag uint8, f float64) []byte {
	x := math.Float64bits(f)
	return append(p, tag<<3|wireBytes, 15,
		tagExprType<<3|wireVarint, byte(mysqlx_expr.Expr_LITERAL),
		tagExprLiteral<<3|wireBytes, 11,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_DOUBLE),
		tagScalarDouble<<3|wireFixed64, byte(x), byte(x>>8), byte(x>>16), byte(x>>24),
		byte(x>>32), byte(x>>40), byte(x>>48), byte(x>>56))
}

func appendExprTime(p []byte, tag uint8, t time.Time) []byte {
	const timeFmt = "2006-01-02 15:04:05.000000000"
	i := len(p)
	p = t.AppendFormat(append(p, tag<<3|wireBytes, 10,
		tagExprType<<3|wireVarint, byte(mysqlx_expr.Expr_LITERAL),
		tagExprLiteral<<3|wireBytes, 6,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_OCTETS),
		tagScalarOctets<<3|wireBytes, 2,
		tagOctetValue<<3|wireBytes, 0), timeFmt)
	n := len(p) - i - 12
	if n+10 >= 0x80 {
		panic("formatted time exceeds 117 bytes in length")
	}
	p[i+11] += byte(n)
	p[i+9] += byte(n)
	p[i+5] += byte(n)
	p[i+1] += byte(n)
	return p
}

func appendExprDuration(p []byte, tag uint8, d time.Duration) []byte {
	i := len(p)
	p = AppendDuration(append(p, tag<<3|wireBytes, 10,
		tagExprType<<3|wireVarint, byte(mysqlx_expr.Expr_LITERAL),
		tagExprLiteral<<3|wireBytes, 6,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_OCTETS),
		tagScalarOctets<<3|wireBytes, 2,
		tagOctetValue<<3|wireBytes, 0), d)
	n := len(p) - i - 12
	p[i+11] += byte(n)
	p[i+9] += byte(n)
	p[i+5] += byte(n)
	p[i+1] += byte(n)
	return p
}

func AppendExprString(p []byte, tag uint8, s string, collation collation.Collation) []byte {
	n := len(s)
	n0 := 1 + sizeVarint(uint(n)) + n // Scalar_String size
	if collation != 0 {
		n0 += 1 + sizeVarint(uint(collation))
	}
	n1 := 3 + sizeVarint(uint(n0)) + n0 // Scalar size
	n2 := 3 + sizeVarint(uint(n1)) + n1 // Expr size

	p, b := slice.ForAppend(p, 1+sizeVarint(uint(n2))+n2)

	i := putUvarint(b[1:], uint64(n2))
	b[0] = tag<<3 | wireBytes
	b = b[1+i:]
	// Expr
	i = putUvarint(b[3:], uint64(n1))
	b[0] = tagExprType<<3 | wireVarint
	b[1] = byte(mysqlx_expr.Expr_LITERAL)
	b[2] = tagExprLiteral<<3 | wireBytes
	b = b[3+i:]
	// Scalar
	i = putUvarint(b[3:], uint64(n0))
	b[0] = tagScalarType<<3 | wireVarint
	b[1] = byte(mysqlx_datatypes.Scalar_V_STRING)
	b[2] = tagScalarString<<3 | wireBytes
	b = b[3+i:]
	// Scalar_String
	if collation != 0 {
		i = putUvarint(b[1:], uint64(collation))
		b[0] = tagStringCollation<<3 | wireVarint
		b = b[1+i:]
	}
	b[0] = tagStringValue<<3 | wireBytes
	i = putUvarint(b[1:], uint64(n))
	copy(b[1+i:], s)
	return p
}

func AppendExprBytes(p []byte, tag uint8, bytes []byte, contentType ContentType) []byte {
	if bytes == nil {
		return appendExprNull(p, tag)
	}
	n := len(bytes)
	n0 := 1 + sizeVarint(uint(n)) + n // Scalar_Octets size
	if contentType != 0 {
		n0 += 1 + sizeVarint(uint(contentType))
	}
	n1 := 3 + sizeVarint(uint(n0)) + n0 // Scalar size
	n2 := 3 + sizeVarint(uint(n1)) + n1 // Expr size

	p, b := slice.ForAppend(p, 1+sizeVarint(uint(n2))+n2)

	i := putUvarint(b[1:], uint64(n2))
	b[0] = tag<<3 | wireBytes
	b = b[1+i:]
	// Any
	i = putUvarint(b[3:], uint64(n1))
	b[0] = tagExprType<<3 | wireVarint
	b[1] = byte(mysqlx_expr.Expr_LITERAL)
	b[2] = tagExprLiteral<<3 | wireBytes
	b = b[3+i:]
	// Scalar
	i = putUvarint(b[3:], uint64(n0))
	b[0] = tagScalarType<<3 | wireVarint
	b[1] = byte(mysqlx_datatypes.Scalar_V_OCTETS)
	b[2] = tagScalarOctets<<3 | wireBytes
	b = b[3+i:]

	// Scalar_Octets
	if contentType != 0 {
		i = putUvarint(b[1:], uint64(contentType))
		b[0] = tagOctetContentType<<3 | wireVarint
		b = b[1+i:]
	}
	i = putUvarint(b[1:], uint64(n))
	b[0] = tagOctetValue<<3 | wireBytes
	copy(b[1+i:], bytes)
	return p
}

func AppendExprBytesString(p []byte, tag uint8, str string, contentType ContentType) []byte {
	n := len(str)
	n0 := 1 + sizeVarint(uint(n)) + n // Scalar_Octets size
	if contentType != 0 {
		n0 += 1 + sizeVarint(uint(contentType))
	}
	n1 := 3 + sizeVarint(uint(n0)) + n0 // Scalar size
	n2 := 3 + sizeVarint(uint(n1)) + n1 // Expr size

	p, b := slice.ForAppend(p, 1+sizeVarint(uint(n2))+n2)

	i := putUvarint(b[1:], uint64(n2))
	b[0] = tag<<3 | wireBytes
	b = b[1+i:]
	// Any
	i = putUvarint(b[3:], uint64(n1))
	b[0] = tagExprType<<3 | wireVarint
	b[1] = byte(mysqlx_expr.Expr_LITERAL)
	b[2] = tagExprLiteral<<3 | wireBytes
	b = b[3+i:]
	// Scalar
	i = putUvarint(b[3:], uint64(n0))
	b[0] = tagScalarType<<3 | wireVarint
	b[1] = byte(mysqlx_datatypes.Scalar_V_OCTETS)
	b[2] = tagScalarOctets<<3 | wireBytes
	b = b[3+i:]

	// Scalar_Octets
	if contentType != 0 {
		i = putUvarint(b[1:], uint64(contentType))
		b[0] = tagOctetContentType<<3 | wireVarint
		b = b[1+i:]
	}
	i = putUvarint(b[1:], uint64(n))
	b[0] = tagOctetValue<<3 | wireBytes
	copy(b[1+i:], str)
	return p
}

type AppendExpr interface {
	AppendExpr(p []byte, tag uint8) ([]byte, error)
}

type AppendExprFunc func([]byte, uint8) ([]byte, error)

func appendExpr(p []byte, tag uint8, v interface{}) ([]byte, error) {
	if v == nil {
		return appendExprNull(p, tag), nil
	}
	switch vv := v.(type) {
	case bool:
		return appendExprBool(p, tag, vv), nil
	case int8:
		return appendExprInt64(p, tag, int64(vv)), nil
	case int16:
		return appendExprInt64(p, tag, int64(vv)), nil
	case int32:
		return appendExprInt64(p, tag, int64(vv)), nil
	case int64:
		return appendExprInt64(p, tag, vv), nil
	case int:
		return appendExprInt64(p, tag, int64(vv)), nil
	case uint8:
		return appendExprUint64(p, tag, uint64(vv)), nil
	case uint16:
		return appendExprUint64(p, tag, uint64(vv)), nil
	case uint32:
		return appendExprUint64(p, tag, uint64(vv)), nil
	case uint64:
		return appendExprUint64(p, tag, vv), nil
	case uint:
		return appendExprUint64(p, tag, uint64(vv)), nil
	case string:
		return AppendExprString(p, tag, vv, 0), nil
	case []byte:
		return AppendExprBytes(p, tag, vv, 0), nil
	case float32:
		return appendExprFloat32(p, tag, vv), nil
	case float64:
		return appendExprFloat64(p, tag, vv), nil
	case time.Time:
		return appendExprTime(p, tag, vv), nil
	case time.Duration:
		return appendExprDuration(p, tag, vv), nil
	case AppendExprFunc:
		return vv(p, tag)
	default:
		if ae, ok := vv.(AppendExpr); ok {
			return ae.AppendExpr(p, tag)
		}
	}
	return p, fmt.Errorf("unknown type %T", v)
}

func AppendExprOperatorV(p []byte, tag uint8, name string, params ...interface{}) ([]byte, error) {
	return AppendExprOperator(p, tag, name, params)
}

func AppendExprOperator(p []byte, tag uint8, name string, params []interface{}) ([]byte, error) {
	i := len(p)
	for j, param := range params {
		var err error
		p, err = appendExpr(p, tagOperatorParam, param)
		if err != nil {
			return p, fmt.Errorf("parameter %d: %w", j, err)
		}
	}
	nParams := len(p) - i
	nOperator := 1 + sizeVarint(uint(len(name))) + len(name) + nParams
	nExpr := 3 + sizeVarint(uint(nOperator)) + nOperator
	p = slice.Insert(p, i, 1+sizeVarint(uint(nExpr))+nExpr-nParams)
	p[i] = tag<<3 | wireBytes
	i++
	i += putUvarint(p[i:], uint64(nExpr))
	p[i] = tagExprType<<3 | wireVarint
	i++
	p[i] = byte(mysqlx_expr.Expr_OPERATOR)
	i++
	p[i] = tagExprOperator<<3 | wireBytes
	i++
	i += putUvarint(p[i:], uint64(nOperator))
	p[i] = tagOperatorName<<3 | wireBytes
	i++
	i += putUvarint(p[i:], uint64(len(name)))
	copy(p[i:], name)
	return p, nil
}

func AppendExprFunctionCallV(p []byte, tag uint8, name string, params ...interface{}) ([]byte, error) {
	return AppendExprFunctionCall(p, tag, name, params)
}

func AppendExprFunctionCall(p []byte, tag uint8, name string, params []interface{}) ([]byte, error) {

	const (
		tagIdentifierName       = 1
		tagIndetifierSchemaNAme = 2
	)

	i := len(p)
	for j, param := range params {
		var err error
		p, err = appendExpr(p, tagFunctionCallParam, param)
		if err != nil {
			return p, fmt.Errorf("parameter %d: %w", j, err)
		}
	}
	nParams := len(p) - i
	nIdentifier := 1 + sizeVarint(uint(len(name))) + len(name)
	nFuncCall := 1 + sizeVarint(uint(nIdentifier)) + nIdentifier + nParams
	nExpr := 3 + sizeVarint(uint(nFuncCall)) + nFuncCall

	p = slice.Insert(p, i, 1+sizeVarint(uint(nExpr))+nExpr-nParams)

	p[i] = tag<<3 | wireBytes
	i++
	i += putUvarint(p[i:], uint64(nExpr))
	p[i] = tagExprType<<3 | wireVarint
	i++
	p[i] = byte(mysqlx_expr.Expr_FUNC_CALL)
	i++
	p[i] = tagExprFunctionCall<<3 | wireBytes
	i++
	i += putUvarint(p[i:], uint64(nFuncCall))
	p[i] = tagFunctionCallName<<3 | wireBytes
	i++
	i += putUvarint(p[i:], uint64(nIdentifier))
	p[i] = tagIdentifierName<<3 | wireBytes
	i++
	i += putUvarint(p[i:], uint64(len(name)))
	copy(p[i:], name)
	return p, nil
}

func AppendExprColumn(p []byte, tag uint8, name string) ([]byte, error) {
	n := len(name)
	n0 := 1 + sizeVarint(uint(n)) + n
	n1 := 3 + sizeVarint(uint(n0)) + n0
	p, b := slice.ForAppend(p, 1+sizeVarint(uint(n1))+n1)

	b[0] = tag<<3 | wireBytes
	i := 1 + putUvarint(b[1:], uint64(n1))
	b[i] = tagExprType<<3 | wireVarint
	i++
	b[i] = byte(mysqlx_expr.Expr_IDENT)
	i++
	b[i] = tagExprIdentifier<<3 | wireBytes
	i++
	i += putUvarint(b[i:], uint64(n0))
	b[i] = tagColumnIdentifierName<<3 | wireBytes
	i++
	i += putUvarint(b[i:], uint64(n))
	copy(b[i:], name)
	return p, nil
}

func AppendExprPlaceholder(p []byte, tag uint8, pos uint32) ([]byte, error) {
	n := sizeVarint32(pos)
	p = append(p, tag<<3|wireBytes, 3+byte(n),
		tagExprType<<3|wireVarint, byte(mysqlx_expr.Expr_PLACEHOLDER),
		tagExprPosition<<3|wireVarint, byte(pos)|0x80, byte(pos>>7)|0x80, byte(pos>>14)|0x80, byte(pos>>21)|0x80, byte(pos>>28))
	n += len(p) - binary.MaxVarintLen32
	p[n-1] &= 0x7F
	return p[:n], nil
}

func AppendExprVariable(p []byte, tag uint8, name string) ([]byte, error) {
	x := len(name)
	n := sizeVarint(uint(x))
	p = append(p, tag<<3|wireBytes, 3+byte(n+x),
		tagExprType<<3|wireVarint, byte(mysqlx_expr.Expr_VARIABLE),
		tagExprVariable<<3|wireBytes, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14)|0x80, byte(x>>21)|0x80, byte(x>>28)|0x80,
		byte(x>>35)|0x80, byte(x>>42)|0x80, byte(x>>49)|0x80, byte(x>>56), 1)
	n += len(p) - binary.MaxVarintLen64
	p[n-1] &= 0x7F
	return append(p[:n], name...), nil
}
