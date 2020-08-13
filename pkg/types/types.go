package types

import (
	"context"
)

type Key = []byte
type Value = []byte
type Timestamp = uint64
type DriverType string

const (
	MinIODriver DriverType = "MinIO"
	TIKVDriver  DriverType = "TIKV"
)

type Store interface {
	Get(ctx context.Context, key Key, timestamp Timestamp) (Value, error)
	BatchGet(ctx context.Context, keys [] Key, timestamp Timestamp) ([]Value, error)
	Set(ctx context.Context, key Key, v Value, timestamp Timestamp) error
	BatchSet(ctx context.Context, keys []Key, v []Value, timestamp Timestamp) error
	Delete(ctx context.Context, key Key, timestamp Timestamp) error
	BatchDelete(ctx context.Context, keys []Key, timestamp Timestamp) error
	Close() error
}
