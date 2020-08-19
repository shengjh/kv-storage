package types

import "context"

type Key = []byte
type Value = []byte
type Timestamp = uint64
type DriverType=string
type SegmentIndex = []byte
type SegmentDL=[]byte

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

type storeEngine interface {
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
	put(ctx context.Context, key Key, value Value, timestamp Timestamp, suffix string)

	GetRow(ctx context.Context, key Key, timestamp Timestamp) Value
	GetRows(ctx context.Context, keys []Key, timestamp Timestamp) []Value

	AddRow(ctx context.Context, key Key, value Value, segment string, timestamp Timestamp) error
	AddRows(ctx context.Context, keys []Key, values []Value, segments []string, timestamp Timestamp) error

	DeleteRow(ctx context.Context, key Key, timestamp Timestamp)
	DeleteRows(ctx context.Context, keys []Key, timestamp Timestamp)

	scanLE(ctx context.Context, key Key, timestamp Timestamp, withValue bool) ([]Timestamp, []Key, []Value)
	scanGE(ctx context.Context, key Key, timestamp Timestamp, withValue bool) ([]Timestamp, []Key, []Value)
	scan(ctx context.Context, key Key, start Timestamp, end Timestamp, withValue bool) ([]Timestamp, []Key, []Value)

	deleteLE(ctx context.Context, key Key, timestamp Timestamp)
	deleteGE(ctx context.Context, key Key, timestamp Timestamp)
	rangeDelete(ctx context.Context, key Key, start Timestamp, end Timestamp)

	LogPut(ctx context.Context, key Key, value Value, timestamp Timestamp, channel int)
	LogFetch(ctx context.Context, start Timestamp, end Timestamp, channels []int)

	GetSegmenIndex(ctx context.Context, segment string) SegmentIndex
	PutSegmentIndex(ctx context.Context, segment string, index SegmentIndex)
	DeleteSegmentIndex(ctx context.Context, segment string)

	GetSegmentDL(ctx context.Context, segment string) SegmentDL
	SetSegmentDL(ctx context.Context, segment string, log SegmentDL)
	DeleteSegmentDL(ctx context.Context, segment string)

}
