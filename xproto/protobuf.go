package xproto

import (
	"encoding/binary"
	"math/bits"
)

// Protobuf WireTypes from "github.com/golang/protobuf/proto"
const (
	wireVarint     = 0
	wireFixed64    = 1
	wireBytes      = 2
	wireStartGroup = 3
	wireEndGroup   = 4
	wireFixed32    = 5
)

var int32Array = [...]int32{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
	20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
	30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
	40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
}

func toPointer32(x int32) *int32 {
	if x >= 0 && int(x) < len(int32Array) {
		return &int32Array[x]
	}
	y := new(int32)
	*y = x
	return y
}

func encodeBool(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func sizeVarint(x uint) int {
	// 9/64 is a good enough approximation of 1/7
	// uint32 cast is to prevent unnecessary 64bit multiplication
	return int(9*uint32(bits.Len(x))+64) / 64
}

func sizeVarint8(x uint8) int {
	return int(x>>7 + 1)
}

func sizeVarint16(x uint16) int {
	return int(9*uint32(bits.Len16(x))+64) / 64
}

func sizeVarint32(x uint32) int {
	return int(9*uint32(bits.Len32(x))+64) / 64
}

func sizeVarint64(x uint64) int {
	return int(9*uint32(bits.Len64(x))+64) / 64
}

func putUvarint(b []byte, x uint64) int {
	if x < 0x80 {
		b[0] = byte(x)
		return 1
	}
	return binary.PutUvarint(b, x)
}

func appendWireString(p []byte, tag uint8, value string) []byte {
	x := uint64(len(value))
	n := sizeVarint64(x)
	p = append(p, tag<<3|wireBytes, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14)|0x80, byte(x>>21)|0x80, byte(x>>28)|0x80,
		byte(x>>35)|0x80, byte(x>>42)|0x80, byte(x>>49)|0x80, byte(x>>56)|0x80, 1)
	n += len(p) - binary.MaxVarintLen64
	p[n-1] &= 0x7F
	return append(p[:n], value...)
}

func appendWireBytes(p []byte, tag uint8, value []byte) []byte {
	x := uint64(len(value))
	n := sizeVarint64(x)
	p = append(p, tag<<3|wireBytes, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14)|0x80, byte(x>>21)|0x80, byte(x>>28)|0x80,
		byte(x>>35)|0x80, byte(x>>42)|0x80, byte(x>>49)|0x80, byte(x>>56)|0x80, 1)
	n += len(p) - binary.MaxVarintLen64
	p[n-1] &= 0x7F
	return append(p[:n], value...)
}
