package xproto

import (
	"encoding/binary"

	"github.com/renthraysk/xtorm/protobuf/mysqlx"
	"github.com/renthraysk/xtorm/slice"
)

func Reset(p []byte, keepOpen bool) []byte {
	const (
		tagResetKeepOpen = 1
	)
	if keepOpen {
		return append(p, 3, 0, 0, 0, byte(mysqlx.ClientMessages_SESS_RESET), tagResetKeepOpen<<3|wireVarint, 1)
	}
	return append(p, 1, 0, 0, 0, byte(mysqlx.ClientMessages_SESS_RESET))
}

const (
	tagStmtExecuteStmt            = 1
	tagStmtExecuteArgs            = 2
	tagStmtExecuteNamespace       = 3
	tagStmtExecuteCompactMetadata = 4
)

func StmtExecute(p []byte, stmt string, args []interface{}) ([]byte, error) {
	x := uint(len(stmt))
	n := len(p)
	p = append(p, 0, 0, 0, 0, byte(mysqlx.ClientMessages_SQL_STMT_EXECUTE),
		tagStmtExecuteStmt<<3|wireBytes, byte(x)|0x80, byte(x>>7)|0x80, byte(x>>14)|0x80, byte(x>>21)|0x80, byte(x>>28)|0x80,
		byte(x>>35)|0x80, byte(x>>42)|0x80, byte(x>>49)|0x80, byte(x>>56)|0x80, 1)
	i := len(p) + sizeVarint(x) - binary.MaxVarintLen64
	p[i-1] &= 0x7F
	p = append(p[:i], stmt...)
	for _, arg := range args {
		var err error
		p, err = appendAny(p, tagStmtExecuteArgs, arg)
		if err != nil {
			return p, err
		}
	}
	binary.LittleEndian.PutUint32(p[n:], uint32(len(p)-n-4))
	return p, nil
}

func Delete(p []byte, name string, criteria AppendExprFunc) ([]byte, error) {
	const (
		tagDeleteCollection = 1
		tagDeleteDataModel  = 2
		tagDeleteCriteria   = 3
	)
	const (
		tagCollectionName   = 1
		tagCollectionSchema = 2
	)
	var err error

	s := len(p)

	n := len(name)
	n1 := 1 + sizeVarint(uint(n)) + n // mysqlx.Collection
	p, b := slice.ForAppend(p, 4+1+1+sizeVarint(uint(n1))+n1)
	i := 6 + putUvarint(b[6:], uint64(n1))
	b[4] = byte(mysqlx.ClientMessages_CRUD_DELETE)
	b[5] = tagDeleteCollection<<3 | wireBytes
	b[i] = tagCollectionName<<3 | wireBytes
	i++
	i += putUvarint(b[i:], uint64(n))
	copy(b[i:], name)
	if criteria != nil {
		p, err = criteria(p, tagDeleteCriteria)
	}
	binary.LittleEndian.PutUint32(p[s:], uint32(len(p)-s-4))
	return p, err
}
