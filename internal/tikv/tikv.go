package tikv_driver

import (
	"context"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"math"
	. "storage/internal/tikv/codec"
	. "storage/pkg/types"
)

type tikvStore struct {
	client *rawkv.Client
}

func NewTikvStore(ctx context.Context) (*tikvStore, error) {
	pdAddrs := []string{"127.0.0.1:2379"}
	conf := config.Default()
	client, err := rawkv.NewClient(ctx, pdAddrs, conf)
	if err != nil {
		return nil, err
	}
	return &tikvStore{
		client: client,
	}, nil
}

func (s *tikvStore) Name() string {
	return "TiKV storage"
}

func (s *tikvStore) Get(ctx context.Context, key Key, timestamp Timestamp) (Value, error) {
	end := keyAddDelimiter(key)
	keys, vals, err := s.client.Scan(ctx, MvccEncode(key, timestamp), MvccEncode(end, math.MaxUint64), 1)
	if err != nil {
		return nil, err
	}
	if keys == nil {
		return nil, nil
	}
	return vals[0], err
}

func (s *tikvStore) Set(ctx context.Context, key Key, v Value, timestamp Timestamp) error {
	codedKey := MvccEncode(key, timestamp)
	err := s.client.Put(ctx, codedKey, v)
	return err
}

func (s *tikvStore) BatchSet(ctx context.Context, keys []Key, v []Value, timestamp Timestamp) error {
	codedKeys := make([]Key, len(keys))
	for i, key := range keys {
		codedKeys[i] = MvccEncode(key, timestamp)
	}
	err := s.client.BatchPut(ctx, codedKeys, v)
	return err
}

func (s *tikvStore) BatchGet(ctx context.Context, keys []Key, timestamp Timestamp) ([]Value, error) {
	var key Key
	var val Value
	var err error
	var vals []Value
	// TODO: When batch size is too large, should use go routine and chain to multi get?
	for _, key = range keys {
		val, err = s.Get(ctx, key, timestamp)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return vals, err
}

func (s *tikvStore) Delete(ctx context.Context, key Key, timestamp Timestamp) error {
	end := keyAddDelimiter(key)
	err := s.client.DeleteRange(ctx, MvccEncode(key, timestamp), MvccEncode(end, uint64(0)))
	return err
}

func (s *tikvStore) BatchDeleteDeprecated(ctx context.Context, keys []Key, timestamp Timestamp) error {
	var key Key
	var err error

	for _, key = range keys {
		err = s.Delete(ctx, key, timestamp)
		if err != nil {
			return err
		}
	}
	return err
}

var batchSize = 100

type batch struct {
	keys   []Key
	values []Value
}

func (s *tikvStore) BatchDelete(ctx context.Context, keys []Key, timestamp Timestamp) error {
	var key Key
	var err error

	keysLen := len(keys)
	numBatch := (keysLen-1)/batchSize + 1
	batches := make([][]Key, numBatch)

	for i := 0; i < numBatch; i++ {
		batchStart := i * batchSize
		batchEnd := batchStart + batchSize
		// the last batch
		if i == numBatch-1 {
			batchEnd = keysLen
		}
		batches[i] = keys[batchStart:batchEnd]
	}

	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan error, len(batches))
	for _, batch := range batches {
		batch1 := batch
		go func() {
			for _, key = range batch1 {
				ch <- s.Delete(ctx, key, timestamp)
			}
		}()
	}

	for i := 0; i < keysLen; i++ {
		if e := <-ch; e != nil {
			cancel()
			// catch the first error
			if err == nil {
				err = e
			}
		}
	}
	return err
}

func (s *tikvStore) Scan(ctx context.Context, start Key, end Key, limit uint32, timestamp Timestamp) ([]Key, []Value, error) {
	panic("implement me")
}

func (s *tikvStore) ReverseScan(ctx context.Context, start Key, end Key, limit uint32, timestamp Timestamp) ([]Key, []Value, error) {
	panic("implement me")
}

func keyAddDelimiter(key Key) Key {
	// TODO: decide delimiter byte, currently set 0x00
	return append(key, byte(0x00))
}

func (s *tikvStore) Close() error {
	return s.client.Close()
}
