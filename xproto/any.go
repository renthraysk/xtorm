package xproto

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"time"

	"github.com/renthraysk/xtorm/collation"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_datatypes"
	"github.com/renthraysk/xtorm/slice"
)

/*
	Byte banging mysql's X Protocol Any protobuf.
*/

// Tags from Any protobuf
const (
	tagAnyType   = 1
	tagAnyScalar = 2
	tagAnyObject = 3
	tagAnyArray  = 4
)

func appendAnyUint8(p []byte, tag uint8, x uint8) []byte {
	if x < 0x80 {
		return append(p, tag<<3|wireBytes, 8,
			tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
			tagAnyScalar<<3|wireBytes, 4,
			tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_UINT),
			tagScalarUint<<3|wireVarint, x)
	}
	return append(p, tag<<3|wireBytes, 9,
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 5,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_UINT),
		tagScalarUint<<3|wireVarint, x, 1)
}

func appendAnyUint16(p []byte, tag uint8, x uint16) []byte {
	n := int(9*uint32(bits.Len16(x))+64) / 64
	p = append(p, tag<<3|wireBytes, 7+byte(n),
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 3+byte(n),
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_UINT),
		tagScalarUint<<3|wireVarint, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14))
	n += len(p) - binary.MaxVarintLen16
	p[n-1] &= 0x7F
	return p[:n]
}

func appendAnyUint32(p []byte, tag uint8, x uint32) []byte {
	n := (9*bits.Len32(x) + 64) / 64 // casts removed to squeak under inline cost limit
	p = append(p, tag<<3|wireBytes, 7+byte(n),
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 3+byte(n),
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_UINT),
		tagScalarUint<<3|wireVarint, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14)|0x80, byte(x>>21)|0x80, byte(x>>28))
	n += len(p) - binary.MaxVarintLen32
	p[n-1] &= 0x7F
	return p[:n]
}

// appendAnyUint appends an Any protobuf representing an uint64 value
// tag refers to the protobuf tag index, and is assumed to be > 0 and < 16
func appendAnyUint64(p []byte, tag uint8, x uint64) []byte {
	n := int(9*uint32(bits.Len64(x))+64) / 64
	p = append(p, tag<<3|wireBytes, 7+byte(n),
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 3+byte(n),
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_UINT),
		tagScalarUint<<3|wireVarint, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14)|0x80, byte(x>>21)|0x80, byte(x>>28)|0x80,
		byte(x>>35)|0x80, byte(x>>42)|0x80, byte(x>>49)|0x80, byte(x>>56), 1)
	n += len(p) - binary.MaxVarintLen64
	p[n-1] &= 0x7F
	return p[:n]
}

func appendAnyInt8(p []byte, tag uint8, v int8) []byte {
	x := (uint8(v) << 1) ^ uint8(v>>7)
	if x < 0x80 {
		return append(p, tag<<3|wireBytes, 8,
			tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
			tagAnyScalar<<3|wireBytes, 4,
			tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_SINT),
			tagScalarSint<<3|wireVarint, x)
	}
	return append(p, tag<<3|wireBytes, 9,
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 5,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_SINT),
		tagScalarSint<<3|wireVarint, x, 1)
}

func appendAnyInt16(p []byte, tag uint8, v int16) []byte {
	x := (uint16(v) << 1) ^ uint16(v>>15)
	n := sizeVarint16(x)
	p = append(p, tag<<3|wireBytes, 7+byte(n),
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 3+byte(n),
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_SINT),
		tagScalarSint<<3|wireVarint, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14))
	n += len(p) - binary.MaxVarintLen16
	p[n-1] &= 0x7F
	return p[:n]
}

func appendAnyInt32(p []byte, tag uint8, v int32) []byte {
	x := (uint32(v) << 1) ^ uint32(v>>31)
	n := sizeVarint32(x)
	p = append(p, tag<<3|wireBytes, 7+byte(n),
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 3+byte(n),
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_SINT),
		tagScalarSint<<3|wireVarint, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14)|0x80, byte(x>>21)|0x80, byte(x>>28))
	n += len(p) - binary.MaxVarintLen32
	p[n-1] &= 0x7F
	return p[:n]
}

// appendAnyInt appends an Any protobuf representing an int64 value
// tag refers to the protobuf tag index, and is assumed to be > 0 and < 16
func appendAnyInt64(p []byte, tag uint8, v int64) []byte {
	x := (uint64(v) << 1) ^ uint64(v>>63)
	n := sizeVarint64(x)
	p = append(p, tag<<3|wireBytes, 7+byte(n),
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 3+byte(n),
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_SINT),
		tagScalarSint<<3|wireVarint, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14)|0x80, byte(x>>21)|0x80, byte(x>>28)|0x80,
		byte(x>>35)|0x80, byte(x>>42)|0x80, byte(x>>49)|0x80, byte(x>>56), 1)
	n += len(p) - binary.MaxVarintLen64
	p[n-1] &= 0x7F
	return p[:n]
}

// appendAnyBytes appends an Any protobuf representing an octet ([]byte) value
// tag refers to the protobuf tag index, and is assumed to be less than 16
func AppendAnyBytes(p []byte, tag uint8, bytes []byte, contentType ContentType) []byte {
	if bytes == nil {
		return appendAnyNull(p, tag)
	}
	n := len(bytes)
	n0 := 1 + sizeVarint(uint(n)) + n // Scalar_Octets size
	if contentType != ContentTypePlain {
		n0 += 1 + sizeVarint(uint(contentType))
	}
	n1 := 3 + sizeVarint(uint(n0)) + n0 // Scalar size
	n2 := 3 + sizeVarint(uint(n1)) + n1 // Any size

	p, b := slice.ForAppend(p, 1+sizeVarint(uint(n2))+n2)

	i := putUvarint(b[1:], uint64(n2))
	b[0] = tag<<3 | wireBytes
	b = b[1+i:]
	// Any
	i = putUvarint(b[3:], uint64(n1))
	b[0] = tagAnyType<<3 | wireVarint
	b[1] = byte(mysqlx_datatypes.Any_SCALAR)
	b[2] = tagAnyScalar<<3 | wireBytes
	b = b[3+i:]
	// Scalar
	i = putUvarint(b[3:], uint64(n0))
	b[0] = tagScalarType<<3 | wireVarint
	b[1] = byte(mysqlx_datatypes.Scalar_V_OCTETS)
	b[2] = tagScalarOctets<<3 | wireBytes
	b = b[3+i:]

	// Scalar_Octets
	if contentType != ContentTypePlain {
		i = putUvarint(b[1:], uint64(contentType))
		b[0] = tagOctetContentType<<3 | wireVarint
		b = b[1+i:]
	}
	i = putUvarint(b[1:], uint64(n))
	b[0] = tagOctetValue<<3 | wireBytes
	copy(b[1+i:], bytes)
	return p
}

// appendAnyBytesString appends an Any protobuf representing an octet (string) value
// tag refers to the protobuf tag index, and is assumed to be less than 16
func AppendAnyBytesString(p []byte, tag uint8, str string, contentType ContentType) []byte {
	n := len(str)
	n0 := 1 + sizeVarint(uint(n)) + n // Scalar_Octets size
	if contentType != ContentTypePlain {
		n0 += 1 + sizeVarint(uint(contentType))
	}
	n1 := 3 + sizeVarint(uint(n0)) + n0 // Scalar size
	n2 := 3 + sizeVarint(uint(n1)) + n1 // Any size

	p, b := slice.ForAppend(p, 1+sizeVarint(uint(n2))+n2)

	i := putUvarint(b[1:], uint64(n2))
	b[0] = tag<<3 | wireBytes
	b = b[1+i:]
	// Any
	i = putUvarint(b[3:], uint64(n1))
	b[0] = tagAnyType<<3 | wireVarint
	b[1] = byte(mysqlx_datatypes.Any_SCALAR)
	b[2] = tagAnyScalar<<3 | wireBytes
	b = b[3+i:]
	// Scalar
	i = putUvarint(b[3:], uint64(n0))
	b[0] = tagScalarType<<3 | wireVarint
	b[1] = byte(mysqlx_datatypes.Scalar_V_OCTETS)
	b[2] = tagScalarOctets<<3 | wireBytes
	b = b[3+i:]

	// Scalar_Octets
	if contentType != ContentTypePlain {
		i = putUvarint(b[1:], uint64(contentType))
		b[0] = tagOctetContentType<<3 | wireVarint
		b = b[1+i:]
	}
	i = putUvarint(b[1:], uint64(n))
	b[0] = tagOctetValue<<3 | wireBytes
	copy(b[1+i:], str)
	return p
}

// appendAnyString appends an Any protobuf representing a string value
// tag refers to the protobuf tag index, and is assumed to be less than 16
func AppendAnyString(p []byte, tag uint8, s string, collation collation.Collation) []byte {
	n := len(s)
	n0 := 1 + sizeVarint(uint(n)) + n // Scalar_String size
	if collation != 0 {
		n0 += 1 + sizeVarint(uint(collation))
	}
	n1 := 3 + sizeVarint(uint(n0)) + n0 // Scalar size
	n2 := 3 + sizeVarint(uint(n1)) + n1 // Any size
	p, b := slice.ForAppend(p, 1+sizeVarint(uint(n2))+n2)
	i := putUvarint(b[1:], uint64(n2))
	b[0] = tag<<3 | wireBytes
	b = b[1+i:]
	// Any
	i = putUvarint(b[3:], uint64(n1))
	b[0] = tagAnyType<<3 | wireVarint
	b[1] = byte(mysqlx_datatypes.Any_SCALAR)
	b[2] = tagAnyScalar<<3 | wireBytes
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
	i = putUvarint(b[1:], uint64(n))
	b[0] = tagStringValue<<3 | wireBytes
	copy(b[1+i:], s)
	return p
}

func appendAnyTime(p []byte, tag uint8, t time.Time) []byte {
	const fmt = "2006-01-02 15:04:05.000000000"

	i := len(p)
	p = t.AppendFormat(append(p, tag<<3|wireBytes, 10,
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 6,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_OCTETS),
		tagScalarOctets<<3|wireBytes, 2,
		tagOctetValue<<3|wireBytes, 0), fmt)
	n := len(p) - i - 12
	if n >= 0x80-10 {
		panic("formatted time exceeds 117 bytes in length")
	}
	p[i+11] += byte(n)
	p[i+9] += byte(n)
	p[i+5] += byte(n)
	p[i+1] += byte(n)
	return p
}

func appendAnyDuration(p []byte, tag uint8, d time.Duration) []byte {
	i := len(p)
	p = AppendDuration(append(p, tag<<3|wireBytes, 10,
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 6,
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

// appendAnyFloat64 appends an Any protobuf representing a float64 value
// tag refers to the protobuf tag index, and is assumed to be > 0 and < 16
func appendAnyFloat64(p []byte, tag uint8, f float64) []byte {
	x := math.Float64bits(f)
	return append(p, tag<<3|wireBytes, 15,
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 11,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_DOUBLE),
		tagScalarDouble<<3|wireFixed64, byte(x), byte(x>>8), byte(x>>16), byte(x>>24),
		byte(x>>32), byte(x>>40), byte(x>>48), byte(x>>56))
}

// appendAnyFloat32 appends an Any protobuf representing a float32 value
// tag refers to the protobuf tag index, and is assumed to be > 0 and < 16
func appendAnyFloat32(p []byte, tag uint8, f float32) []byte {
	x := math.Float32bits(f)
	return append(p, tag<<3|wireBytes, 11,
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 7,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_FLOAT),
		tagScalarFloat<<3|wireFixed32, byte(x), byte(x>>8), byte(x>>16), byte(x>>24))
}

// appendAnyBool appends an Any protobuf representing a bool value
// tag refers to the protobuf tag index, and is assumed to be > 0 and < 16
func appendAnyBool(p []byte, tag uint8, b bool) []byte {
	return append(p, tag<<3|wireBytes, 8,
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 4,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_BOOL),
		tagScalarBool<<3|wireVarint, encodeBool(b))
}

// appendAnyNull appends an Any protobuf representing a NULL/nil value
// tag refers to the protobuf tag index, and is assumed to be > 0 and < 16
func appendAnyNull(p []byte, tag uint8) []byte {
	return append(p, tag<<3|wireBytes, 6,
		tagAnyType<<3|wireVarint, byte(mysqlx_datatypes.Any_SCALAR),
		tagAnyScalar<<3|wireBytes, 2,
		tagScalarType<<3|wireVarint, byte(mysqlx_datatypes.Scalar_V_NULL))
}

type AppendAny interface {
	AppendAny(p []byte, tag uint8) ([]byte, error)
}

func appendAny(p []byte, tag uint8, value interface{}) ([]byte, error) {
	if value == nil {
		return appendAnyNull(p, tag), nil
	}
typeSwitch:
	switch v := value.(type) {
	case int:
		if bits.UintSize == 32 {
			return appendAnyInt32(p, tag, int32(v)), nil
		}
		return appendAnyInt64(p, tag, int64(v)), nil
	case int8:
		return appendAnyInt8(p, tag, v), nil
	case int16:
		return appendAnyInt16(p, tag, v), nil
	case int32:
		return appendAnyInt32(p, tag, v), nil
	case int64:
		return appendAnyInt64(p, tag, v), nil
	case uint:
		if bits.UintSize == 32 {
			return appendAnyUint32(p, tag, uint32(v)), nil
		}
		return appendAnyUint64(p, tag, uint64(v)), nil
	case uint8:
		return appendAnyUint8(p, tag, v), nil
	case uint16:
		return appendAnyUint16(p, tag, v), nil
	case uint32:
		return appendAnyUint32(p, tag, v), nil
	case uint64:
		return appendAnyUint64(p, tag, v), nil
	case string:
		return AppendAnyString(p, tag, v, 0), nil
	case []byte:
		return AppendAnyBytes(p, tag, v, ContentTypePlain), nil
	case bool:
		return appendAnyBool(p, tag, v), nil
	case float32:
		return appendAnyFloat32(p, tag, v), nil
	case float64:
		return appendAnyFloat64(p, tag, v), nil
	case time.Time:
		return appendAnyTime(p, tag, v), nil
	case time.Duration:
		return appendAnyDuration(p, tag, v), nil

	default:
		if ae, ok := v.(AppendAny); ok {
			return ae.AppendAny(p, tag)
		}

		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Ptr:
			if rv.IsNil() {
				return appendAnyNull(p, tag), nil
			}
			value = rv.Elem().Interface()
			goto typeSwitch

		default:
			return nil, fmt.Errorf("unsupported type %T, a %s", value, rv.Kind())
		}
	}
}
