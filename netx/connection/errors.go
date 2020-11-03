package connection

import (
	"fmt"

	"github.com/renthraysk/xtorm/protobuf/mysqlx"
)

type MySqlXError struct {
	Severity mysqlx.Error_Severity
	Code     uint32
	SqlState string
	Msg      string
}

func (e *MySqlXError) Error() string {
	return fmt.Sprintf("%s %d (%s): %s", e.Severity.String(), e.Code, e.SqlState, e.Msg)
}

func (e *MySqlXError) IsFatal() bool {
	return e.Severity == mysqlx.Error_FATAL
}

type errorString string

func (e errorString) Error() string {
	return string(e)
}

const (
	ErrUnexpectedAuthenticateContinue = errorString("unexpected AuthenticateContinue")
)

type ErrRequireAuthenticateContinue struct {
	AuthData []byte
}

func (ErrRequireAuthenticateContinue) Error() string { return "require AuthenticateContinue" }
