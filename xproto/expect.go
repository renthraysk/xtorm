package xproto

import (
	"encoding/binary"

	"github.com/renthraysk/xtorm/protobuf/mysqlx"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_expect"
)

type OpenCondition func(b []byte) []byte

const (
	tagOpenConditionKey   = 1
	tagOpenConditionValue = 2
)
const (
	tagExpectOpenOp   = 1
	tagExpectOpenCond = 2
)

func openConditionExpectNoErrorTrue(b []byte) []byte {
	return append(b, tagExpectOpenCond<<3|wireBytes, 5,
		tagOpenConditionKey<<3|wireVarint, byte(mysqlx_expect.Open_Condition_EXPECT_NO_ERROR),
		tagOpenConditionValue<<3|wireBytes, 1, '1')
}

func openConditionExpectNoErrorFalse(b []byte) []byte {
	return append(b, tagExpectOpenCond<<3|wireBytes, 5,
		tagOpenConditionKey<<3|wireVarint, byte(mysqlx_expect.Open_Condition_EXPECT_NO_ERROR),
		tagOpenConditionValue<<3|wireBytes, 1, '0')
}

func OpenConditionExpectNoError(value bool) OpenCondition {
	if value {
		return openConditionExpectNoErrorTrue
	}
	return openConditionExpectNoErrorFalse
}

func OpenConditionExpectFieldExists(field string) OpenCondition {
	return func(p []byte) []byte {
		n := len(field)
		n0 := 3 + sizeVarint(uint(n)) + n
		p = append(p, tagExpectOpenCond<<3|wireBytes, byte(n0)|0x80, byte(n0>>7)|0x80, byte(n0>>14)|0x80, byte(n0>>21)|0x80, byte(n0>>28))
		i := len(p) - binary.MaxVarintLen32 + sizeVarint(uint(n0))
		p[i-1] &= 0x7F
		p = append(p[:i], tagOpenConditionKey<<3|wireVarint, byte(mysqlx_expect.Open_Condition_EXPECT_FIELD_EXIST),
			tagOpenConditionValue<<3|wireBytes, byte(n)|0x80, byte(n>>7)|0x80, byte(n>>14)|0x80, byte(n>>21)|0x80, byte(n>>28))
		i = len(p) - binary.MaxVarintLen32 + sizeVarint(uint(n))
		p[i-1] &= 0x7F
		return append(p[:i], field...)
	}
}

type OpenCtxOperation uint8

const (
	OpenExpectCtxCopyPrev = OpenCtxOperation(mysqlx_expect.Open_EXPECT_CTX_COPY_PREV)
	OpenExpectCtxEmpty    = OpenCtxOperation(mysqlx_expect.Open_EXPECT_CTX_EMPTY)
)

func ExpectOpen(p []byte, op OpenCtxOperation, conditions ...OpenCondition) []byte {
	i := len(p)
	p = append(p, 0, 0, 0, 0, byte(mysqlx.ClientMessages_EXPECT_OPEN),
		tagExpectOpenOp<<3|wireVarint, byte(op))
	for _, appendCondition := range conditions {
		p = appendCondition(p)
	}
	binary.LittleEndian.PutUint32(p[i:], uint32(len(p)-i-4))
	return p
}

func ExpectClose(p []byte) []byte {
	return append(p, 1, 0, 0, 0, byte(mysqlx.ClientMessages_EXPECT_CLOSE))
}
