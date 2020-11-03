package xproto

import (
	"encoding/binary"

	"github.com/renthraysk/xtorm/protobuf/mysqlx"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_prepare"
	"github.com/renthraysk/xtorm/slice"
)

func Prepare(p []byte, id uint32, stmt string) []byte {
	const (
		tagPrepareStmtId = 1
		tagPrepareStmt   = 2

		tagPrepareOneOfType    = 1
		tagPrepareOneOfExecute = 6
	)
	n := len(stmt)
	n0 := 1 + sizeVarint(uint(n)) + n                      // Stmt
	n1 := 3 + sizeVarint(uint(n0)) + n0                    // PrepareOneOf
	n2 := 2 + sizeVarint32(id) + sizeVarint(uint(n1)) + n1 // Prepare
	b := p
	p, b = slice.ForAppend(p, 4+1+n2)

	i := 6 + putUvarint(b[6:], uint64(id))
	binary.LittleEndian.PutUint32(b, uint32(1+n2))
	b[4] = byte(mysqlx.ClientMessages_PREPARE_PREPARE)
	b[5] = tagPrepareStmtId<<3 | wireVarint
	b[i] = tagPrepareStmt<<3 | wireBytes
	i++
	i += putUvarint(b[i:], uint64(n1))
	b[i] = tagPrepareOneOfType<<3 | wireVarint
	b[1+i] = byte(mysqlx_prepare.Prepare_OneOfMessage_STMT)
	b[2+i] = tagPrepareOneOfExecute<<3 | wireBytes
	i += 3
	i += putUvarint(b[i:], uint64(n0))
	b[i] = tagStmtExecuteStmt<<3 | wireBytes
	i++
	i += putUvarint(b[i:], uint64(n))
	copy(b[i:], stmt)
	return p
}

// Execute appends header and mysqlx_prepare.Execute protobuf to execute a prepared statement with id, and given set ofg args.
func Execute(p []byte, id uint32, args []interface{}) ([]byte, error) {
	const (
		tagExecuteStmtId = 1
		tagExecuteArgs   = 2
	)
	n := len(p)
	p = append(p[:], 0, 0, 0, 0, byte(mysqlx.ClientMessages_PREPARE_EXECUTE),
		tagExecuteStmtId<<3|wireVarint, byte(id)|0x80, byte(id>>7)|0x80, byte(id>>14)|0x80, byte(id>>21)|0x80, byte(id>>28))
	i := len(p) - binary.MaxVarintLen32 + sizeVarint32(id)
	p[i-1] &= 0x7F
	p = p[:i]
	for _, arg := range args {
		var err error
		p, err = appendAny(p, tagExecuteArgs, arg)
		if err != nil {
			return p, err
		}
	}
	binary.LittleEndian.PutUint32(p[n:], uint32(len(p)-n-4))
	return p, nil
}

// Deallocate appends header and mysqlx_prepare.Deallocate protobuf to dellocate prepare statement with id to p
func Deallocate(p []byte, id uint32) []byte {
	const (
		tagDeallocateStmtId = 1
	)
	n := sizeVarint32(id)
	p = append(p[:], 2+uint8(n), 0, 0, 0, byte(mysqlx.ClientMessages_PREPARE_DEALLOCATE),
		tagDeallocateStmtId<<3|wireVarint, uint8(id)|0x80, uint8(id>>7)|0x80, uint8(id>>14)|0x80, uint8(id>>21)|0x80, uint8(id>>28))
	n += len(p) - binary.MaxVarintLen32
	p[n-1] &= 0x7F
	return p[:n]
}
