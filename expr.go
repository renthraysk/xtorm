package xtorm

import (
	"github.com/renthraysk/xtorm/collation"
	"github.com/renthraysk/xtorm/xproto"
)

type exprFunc = xproto.AppendExprFunc

// String wraps a string and collation to make it suitable for passing as an argument or expression
type String struct {
	Value     string
	Collation collation.Collation
}

func (s String) AppendAny(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendAnyString(p, tag, s.Value, s.Collation), nil
}

func (s String) AppendExpr(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendExprString(p, tag, s.Value, s.Collation), nil
}

type xml []byte

// XML wraps a XML byte slice to make it suitable for passing as an argument or expression
func XML(b []byte) xml { return xml(b) }

func (x xml) AppendAny(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendAnyBytes(p, tag, x, xproto.ContentTypeXML), nil
}

func (x xml) AppendExpr(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendExprBytes(p, tag, x, xproto.ContentTypeXML), nil
}

type xmlString string

// XMLString wraps a XML string to make it suitable for passing as an argument or expression
func XMLString(s string) xmlString { return xmlString(s) }

func (x xmlString) AppendAny(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendAnyBytesString(p, tag, string(x), xproto.ContentTypeXML), nil
}

func (x xmlString) AppendExpr(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendExprBytesString(p, tag, string(x), xproto.ContentTypeXML), nil
}

type json []byte

// JSON wraps a json byte slice to make it suitable for passing as an argument or expression
func JSON(b []byte) json { return json(b) }

func (j json) AppendAny(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendAnyBytes(p, tag, j, xproto.ContentTypeJSON), nil
}

func (j json) AppendExpr(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendExprBytes(p, tag, j, xproto.ContentTypeJSON), nil
}

type jsonString string

// JSONString wraps a json string to make it suitable for passing as an argument or expression
func JSONString(s string) jsonString { return jsonString(s) }

func (j jsonString) AppendAny(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendAnyBytesString(p, tag, string(j), xproto.ContentTypeJSON), nil
}

func (j jsonString) AppendExpr(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendExprBytesString(p, tag, string(j), xproto.ContentTypeJSON), nil
}

type geometry []byte

func Geometry(b []byte) geometry { return geometry(b) }

func (g geometry) AppendAny(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendAnyBytes(p, tag, g, xproto.ContentTypeGeometry), nil
}

func (g geometry) AppendExpr(p []byte, tag uint8) ([]byte, error) {
	return xproto.AppendExprBytes(p, tag, g, xproto.ContentTypeGeometry), nil
}

func Column(name string) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprColumn(p, tag, name)
	}
}

// Operations

func Default() exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "default")
	}
}

// Unary operators

// Not returns expression NOT a. See https://dev.mysql.com/doc/refman/8.0/en/logical-operators.html#operator_not
func Not(a interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "!", a)
	}
}

func Plus(a interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "sign_plus", a)
	}
}

func Minus(a interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "sign_minus", a)
	}
}

// Binary operators

// And returns expression a AND b. See https://dev.mysql.com/doc/refman/8.0/en/logical-operators.html#operator_and
func And(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "&&", a, b)
	}
}

// Or returns expression a OR b. See https://dev.mysql.com/doc/refman/8.0/en/logical-operators.html#operator_or
func Or(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "||", a, b)
	}
}

// Xor returns expression a XOR b. See https://dev.mysql.com/doc/refman/8.0/en/logical-operators.html#operator_xor
func Xor(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "xor", a, b)
	}
}

// Eq returns expression a = b. See https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_equal
func Eq(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "==", a, b)
	}
}

// Neq returns expression a != b. See https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_not-equal
func Neq(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "!=", a, b)
	}
}

// Lte returns expression a <= b. See https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_less-than-or-equal
func Lte(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "<=", a, b)
	}
}

// Lt returns expression a < b. See https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_less-than
func Lt(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "<", a, b)
	}
}

// Gt returns expression a > b. See https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_greater-than
func Gt(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, ">", a, b)
	}
}

// Gte returns expression a >= b. See https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_greater-than-or-equal
func Gte(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, ">=", a, b)
	}
}

// BitAnd returns expression a & b. See https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#operator_bitwise-and
func BitAnd(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "&", a, b)
	}
}

// BitOr returns expression a | b. See https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#operator_bitwise-or
func BitOr(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "|", a, b)
	}
}

// BitXor returns expression a ^ b. See https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#operator_bitwise-xor
func BitXor(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "^", a, b)
	}
}

// LeftShift returns expression a << b. See https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#operator_left-shift
func LeftShift(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "<<", a, b)
	}
}

// RightShift returns expression a >> b. See https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#operator_right-shift
func RightShift(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, ">>", a, b)
	}
}

// Add returns an expression of a + b. See https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_plus
func Add(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "+", a, b)
	}
}

// Sub returns an expression of a - b. See https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_minus
func Sub(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "-", a, b)
	}
}

// Mul returns an expression of a * b. See https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_times
func Mul(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "*", a, b)
	}
}

// Div returns an expression of a / b. See https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_divide
func Div(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "/", a, b)
	}
}

// IntDiv returns an expression of a div b. See https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_div
func IntDiv(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "div", a, b)
	}
}

// Mod returns an expression of a % b, or a MOD b. See https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_mod
func Mod(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "%", a, b)
	}
}

func Is(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "is", a, b)
	}
}

func IsNot(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "is_not", a, b)
	}
}

func Regexp(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "rexgexp", a, b)
	}
}

func NotRegexp(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "not_regexp", a, b)
	}
}

func Like(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "like", a, b)
	}
}

func NotLike(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "not_like", a, b)
	}
}

func ContIn(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "cont_in", a, b)
	}
}

func NotContIn(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "not_cont_in", a, b)
	}
}

func Overlaps(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "overlaps", a, b)
	}
}

func NotOverlaps(a, b interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "not_overlaps", a, b)
	}
}

// Ternary operators....

// Between returns expressions a BETWEEN b AND c
func Between(a, b, c interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "between", a, b, c)
	}
}

// NotBetween returns expressions a NOT BETWEEN b AND c
func NotBetween(a, b, c interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "not_between", a, b, c)
	}
}

// In returns expression a IN (b...)
func In(a interface{}, b []interface{}) exprFunc {
	v := make([]interface{}, 1+len(b))
	v[0] = a
	copy(v[1:], b)
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperator(p, tag, "in", v)
	}
}

// InV vararg variant of In()
func InV(a interface{}, b ...interface{}) exprFunc {
	return In(a, b)
}

// NotIn returns expression a NOT IN (b...)
func NotIn(a interface{}, b []interface{}) exprFunc {
	v := make([]interface{}, 1+len(b))
	v[0] = a
	copy(v[1:], b)
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperator(p, tag, "not_in", v)
	}
}

// NotInV vararg variant of NotIn()
func NotInV(a interface{}, b ...interface{}) exprFunc {
	return NotIn(a, b)
}

type DateUnit string

const (
	UnitMicroSecond       DateUnit = "MICROSECOND"
	UnitSecond            DateUnit = "SECOND"
	UnitMinute            DateUnit = "MINUTE"
	UnitHour              DateUnit = "HOUR"
	UnitDay               DateUnit = "DAY"
	UnitWeek              DateUnit = "WEEK"
	UnitMonth             DateUnit = "MONTH"
	UnitQuarter           DateUnit = "QUARTER"
	UnitYear              DateUnit = "YEAR"
	UnitSecondMicrosecond DateUnit = "SECOND_MICROSECOND"
	UnitMinuteMicrosecond DateUnit = "MINUTE_MICROSECOND"
	UnitMinuteSecond      DateUnit = "MINUTE_SECOND"
	UnitHourMicrosecond   DateUnit = "HOUR_MICROSECOND"
	UnitHourSecond        DateUnit = "HOUR_SECOND"
	UnitHourMinute        DateUnit = "HOUR_MINUTE"
	UnitDayMicrosecond    DateUnit = "DAY_MICROSECOND"
	UnitDaySecond         DateUnit = "DAY_SECOND"
	UnitDayMinute         DateUnit = "DAY_MINUTE"
	UnitDayHour           DateUnit = "DAY_HOUR"
)

// DateAdd See https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-add
func DateAdd(a, b interface{}, c DateUnit) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "date_add", a, b, []byte(c))
	}
}

// DateSub See https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-sub
func DateSub(a, b interface{}, c DateUnit) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprOperatorV(p, tag, "date_sub", a, b, []byte(c))
	}
}

// Functions

// Concat invokes MySQL's CONCAT() function. See
func Concat(a ...interface{}) exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprFunctionCall(p, tag, "CONCAT", a)
	}
}

// RowCount invokes MySQL's ROW_COUNT() function. See https://dev.mysql.com/doc/refman/8.0/en/information-functions.html#function_row-count
func RowCount() exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprFunctionCallV(p, tag, "ROW_COUNT")
	}
}

// LastInsertID invokes MySQL's LAST_INSERT_ID() function. See https://dev.mysql.com/doc/refman/8.0/en/information-functions.html#function_last-insert-id
func LastInsertID() exprFunc {
	return func(p []byte, tag uint8) ([]byte, error) {
		return xproto.AppendExprFunctionCallV(p, tag, "LAST_INSERT_ID")
	}
}
