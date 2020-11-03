module github.com/renthraysk/xtorm

go 1.14

require (
	github.com/go-sql-driver/mysql v1.5.0
	github.com/golang/protobuf v1.4.0
	github.com/renthraysk/xtorm/protobuf v0.0.0-00010101000000-000000000000
)

replace github.com/renthraysk/xtorm/protobuf => ./protobuf
