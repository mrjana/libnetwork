//go:generate protoc --gofast_out=plugins=grpc:. -I.:$HOME/go/src/:$HOME/go/src/github.com/gogo/protobuf store.proto

package store
