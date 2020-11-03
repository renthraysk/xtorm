package xproto

import (
	"github.com/renthraysk/xtorm/protobuf/mysqlx"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_crud"
	"github.com/renthraysk/xtorm/slice"
)

const (
	tagUpdateCollection      = 2
	tagUpdateDataModel       = 3
	tagUpdateCriteria        = 4
	tagUpdateArgs            = 8
	tagUpdateOrder           = 6
	tagUpdateUpdateOperation = 7
	tagUpdateLimit           = 5
	tagUpdateLimitExpr       = 9
)

func Update(p []byte, name string, criteria AppendExprFunc) ([]byte, error) {
	n := len(name)
	n1 := 1 + sizeVarint(uint(n)) + n // Collection size
	p, b := slice.ForAppend(p, 4+1+3+sizeVarint(uint(n1))+n1)
	i := 6 + putUvarint(b[6:], uint64(n1))
	b[4] = byte(mysqlx.ClientMessages_CRUD_UPDATE)
	b[5] = tagUpdateCollection<<3 | wireBytes
	b[i] = tagCollectionName<<3 | wireBytes
	i++
	i += putUvarint(b[i:], uint64(n))
	i += copy(b[i:], name)
	b[i] = tagUpdateDataModel<<3 | wireVarint
	i++
	b[i] = byte(mysqlx_crud.DataModel_TABLE)
	if criteria != nil {
		return criteria(p, tagUpdateCriteria)
	}
	return p, nil
}

func AppendUpdateSet(p []byte, name string, value interface{}) ([]byte, error) {

	const (
		tagUpdateOperationSource    = 1
		tagUpdateOperationOperation = 2
		tagUpdateOperationValue     = 3

		tagColumnIdentifierDocumentPath = 1
		tagColumnIdentifierName         = 2
		tagColumnIdentifierTableName    = 3
		tagColumnIdentifierSchema       = 4
	)

	i := len(p)
	p, err := appendExpr(p, tagUpdateOperationValue, value)
	if err != nil {
		return p, err
	}
	nV := len(p) - i
	n := len(name)
	n1 := 1 + sizeVarint(uint(n)) + n        // ColumnIdentifier size
	n2 := 3 + sizeVarint(uint(n1)) + n1 + nV // OperationSource size

	p = slice.Insert(p, i, 1+sizeVarint(uint(n2))+n2-nV)
	p[i] = tagUpdateUpdateOperation<<3 | wireBytes
	i++
	i += putUvarint(p[i:], uint64(n2))
	p[i] = tagUpdateOperationSource<<3 | wireBytes
	i++
	i += putUvarint(p[i:], uint64(n1))
	p[i] = tagColumnIdentifierName<<3 | wireBytes
	i++
	i += putUvarint(p[i:], uint64(n))
	i += copy(p[i:], name)
	p[i] = tagUpdateOperationOperation<<3 | wireVarint
	i++
	p[i] = byte(mysqlx_crud.UpdateOperation_SET)
	return p, nil
}
