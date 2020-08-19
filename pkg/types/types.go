package types

import "context"

type Key = []byte
type Value = []byte
type Timestamp = uint64
type DriverType string

const (
	MinIODriver DriverType = "MinIO"
	TIKVDriver  DriverType = "TIKV"
)

/*
type Store interface {
	Get(ctx context.Context, key Key, timestamp Timestamp) (Value, error)
	BatchGet(ctx context.Context, keys [] Key, timestamp Timestamp) ([]Value, error)
	Set(ctx context.Context, key Key, v Value, timestamp Timestamp) error
	BatchSet(ctx context.Context, keys []Key, v []Value, timestamp Timestamp) error
	Delete(ctx context.Context, key Key, timestamp Timestamp) error
	BatchDelete(ctx context.Context, keys []Key, timestamp Timestamp) error
	Close() error
}
*/

type StoreEngine interface {
	PUT(ctx context.Context, key Key, value Value)
	GET(ctx context.Context, key Key) Value

	GetLatest(ctx context.Context, key Key, withValue bool) (Key, Value)
	GetAll(ctx context.Context, key Key, withValue bool) ([]Key, []Value)
	Scan(ctx context.Context, keyStart Key, keyEnd Key, withValue bool) ([]Key, []Value)

	Delete(ctx context.Context, key Key)
	DeleteAll(ctx context.Context, key Key)
	RangeDelete(ctx context.Context, keyStart Key, keyEnd Key)
}

type Store interface {
	RawBatchGet(ctx context.Context, key Key) []Value
	RawGet(ctx context.Context, key Key) Value
	RawPut(ctx context.Context, key Key, value Value)

	RawDeleteAll(ctx context.Context, key Key)
	RawDelete(ctx context.Context, key Key)

	Get(ctx context.Context, key Key, timestamp Timestamp) Value
	BatchGet(ctx context.Context, keys []Key, timestamp Timestamp) []Value

	GetAll(ctx context.Context, key Key, withValue bool) ([]Timestamp, []Key, []Value)

	ScanLE(ctx context.Context, key Key, timestamp Timestamp, withValue bool) ([]Timestamp, []Key, []Value)
	ScanGE(ctx context.Context, key Key, timestamp Timestamp, withValue bool) ([]Timestamp, []Key, []Value)
	ScanRange(ctx context.Context, key Key, start Timestamp, end Timestamp, withValue bool) ([]Timestamp, []Key, []Value)

	PUT(ctx context.Context, key Key, value Value, timestamp Timestamp, suffix string)

	DeleteLE(ctx context.Context, key Key, timestamp Timestamp)
	DeleteGE(ctx context.Context, key Key, timestamp Timestamp)
	RangeDelete(ctx context.Context, key Key, start Timestamp, end Timestamp)

	LogPut(ctx context.Context, key Key, value Value, timestamp Timestamp, suffix string)
	LogFetch(ctx context.Context, start Timestamp, end Timestamp, channels []int)
}
