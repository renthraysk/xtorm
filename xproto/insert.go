package xproto

import (
	"fmt"

	"github.com/renthraysk/xtorm/protobuf/mysqlx"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_crud"
	"github.com/renthraysk/xtorm/slice"
)

const (
	tagInsertCollection = 1
	tagInsertDataModel  = 2
	tagInsertProjection = 3
	tagInsertRow        = 4
	tagInsertArgs       = 5
	tagInsertUpsert     = 6

	tagCollectionName   = 1
	tagCollectionSchema = 2

	tagColumnName = 1

	tagTypedRowField = 1
)

func Insert(p []byte, tableName string, names []string) []byte {
	var b []byte

	n := len(tableName)
	n0 := 1 + sizeVarint(uint(n)) + n
	p, b = slice.ForAppend(p, 5+3+sizeVarint(uint(n0))+n0)
	b[4] = byte(mysqlx.ClientMessages_CRUD_INSERT)
	b[5] = tagInsertCollection<<3 | wireBytes
	i := 6 + putUvarint(b[6:], uint64(n0))
	b[i] = tagCollectionName<<3 | wireBytes
	i++
	i += putUvarint(b[i:], uint64(n))
	i += copy(b[i:], tableName)
	b[i] = tagInsertDataModel<<3 | wireVarint
	i++
	b[i] = byte(mysqlx_crud.DataModel_TABLE)
	for _, name := range names {
		var b []byte
		n := len(name)
		n0 := 1 + sizeVarint(uint(n)) + n

		p, b = slice.ForAppend(p, 1+sizeVarint(uint(n0))+n0)
		b[0] = tagInsertProjection<<3 | wireBytes
		j := 1 + putUvarint(b[1:], uint64(n0))
		b[j] = tagColumnName<<3 | wireBytes
		j++
		j += putUvarint(b[j:], uint64(n))
		copy(b[j:], name)
	}
	return p
}

func AppendInsertRow(p []byte, row []interface{}) ([]byte, error) {
	const rowSizeSize = 2

	p = append(p, tagInsertRow<<3|wireBytes, 0, 0) // rowSizeSize bytes
	i := len(p)
	for j, v := range row {
		var err error
		p, err = appendExpr(p, tagTypedRowField, v)
		if err != nil {
			return p, fmt.Errorf("failed to marshal column %d: %w", j, err)
		}
	}
	n := uint(len(p) - i)
	if d := sizeVarint(n) - rowSizeSize; d != 0 {
		p = append(p[:i+d], p[i:]...)
	}
	putUvarint(p[i-rowSizeSize:], uint64(n))
	return p, nil
}
