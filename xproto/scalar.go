package xproto

import (
	"strconv"
	"time"

	"github.com/renthraysk/xtorm/protobuf/mysqlx_resultset"
)

type ContentType uint32

const (
	ContentTypePlain    ContentType = 0
	ContentTypeGeometry             = ContentType(mysqlx_resultset.ContentType_BYTES_GEOMETRY)
	ContentTypeJSON                 = ContentType(mysqlx_resultset.ContentType_BYTES_JSON)
	ContentTypeXML                  = ContentType(mysqlx_resultset.ContentType_BYTES_XML)
)

const (
	// Tags from Scalar protobuf
	tagScalarType   = 1
	tagScalarSint   = 2
	tagScalarUint   = 3
	_               // Null
	tagScalarOctets = 5
	tagScalarDouble = 6
	tagScalarFloat  = 7
	tagScalarBool   = 8
	tagScalarString = 9

	// Tags from Scalar_String protobuf
	tagStringValue     = 1
	tagStringCollation = 2

	// Tags from Scalar_Octets protobuf
	tagOctetValue       = 1
	tagOctetContentType = 2
)

const smallsString = "00010203040506070809" +
	"10111213141516171819" +
	"20212223242526272829" +
	"30313233343536373839" +
	"40414243444546474849" +
	"50515253545556575859"

func AppendDuration(p []byte, d time.Duration) []byte {
	if d < 0 {
		d = -d
		p = append(p, '-')
	}
	i := 2 * (uint(d/time.Minute) % 60)
	j := 2 * (uint(d/time.Second) % 60)
	return append(strconv.AppendUint(p, uint64(d/time.Hour), 10), ':', smallsString[i], smallsString[i+1], ':', smallsString[j], smallsString[j+1])
}
