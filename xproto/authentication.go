package xproto

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/protobuf/proto"

	"github.com/renthraysk/xtorm/protobuf/mysqlx"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_connection"
	"github.com/renthraysk/xtorm/protobuf/mysqlx_datatypes"
)

func anyType(x mysqlx_datatypes.Any_Type) *mysqlx_datatypes.Any_Type {
	return (*mysqlx_datatypes.Any_Type)(toPointer32(int32(x)))
}

func scalarType(x mysqlx_datatypes.Scalar_Type) *mysqlx_datatypes.Scalar_Type {
	return (*mysqlx_datatypes.Scalar_Type)(toPointer32(int32(x)))
}

func CapabilitySet(name string, enable bool) ([]byte, error) {
	cs := mysqlx_connection.CapabilitiesSet{
		Capabilities: &mysqlx_connection.Capabilities{
			Capabilities: []*mysqlx_connection.Capability{
				{
					Name: &name,
					Value: &mysqlx_datatypes.Any{
						Type:   anyType(mysqlx_datatypes.Any_SCALAR),
						Scalar: &mysqlx_datatypes.Scalar{Type: scalarType(mysqlx_datatypes.Scalar_V_BOOL), VBool: &enable},
					},
				},
			},
		},
	}

	n := proto.Size(&cs)
	b := make([]byte, 5+n)
	binary.LittleEndian.PutUint32(b, uint32(n+1))
	b[4] = byte(mysqlx.ClientMessages_CON_CAPABILITIES_SET)
	buf := proto.NewBuffer(b[:5])
	if err := buf.Marshal(&cs); err != nil {
		return nil, fmt.Errorf("failed to marshal CapabilitiesSet: %w", err)
	}
	return b, nil
}

func AuthenticateStart(p []byte, mechName string, authData []byte) []byte {
	const (
		tagAuthenticateStartMechName = 1
		tagAuthenticateStartAuthData = 2
	)
	p = append(p[len(p):], 0, 0, 0, 0, byte(mysqlx.ClientMessages_SESS_AUTHENTICATE_START))
	p = appendWireString(p, tagAuthenticateStartMechName, mechName)
	if len(authData) > 0 {
		p = appendWireBytes(p, tagAuthenticateStartAuthData, authData)
	}
	binary.LittleEndian.PutUint32(p[:], uint32(len(p)-4))
	return p
}

func AuthenticateContinue(p []byte, authData []byte) []byte {
	const (
		tagAuthenticateContinueAuthData = 1
	)
	p = append(p[len(p):], 0, 0, 0, 0, byte(mysqlx.ClientMessages_SESS_AUTHENTICATE_CONTINUE))
	p = appendWireBytes(p, tagAuthenticateContinueAuthData, authData)
	binary.LittleEndian.PutUint32(p[:], uint32(len(p)-4))
	return p
}
