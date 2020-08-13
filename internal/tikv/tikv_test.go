package tikv_driver

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/config"
	"math"
	"os"
	"testing"
	. "storage/pkg/types"
)

//var store  tikvDriver.tikvStore
var store *tikvStore

var (
	conf = config.Default()
)

func TestMain(m *testing.M) {
	store, _ = NewTikvStore(context.Background())
	exitCode := m.Run()
	_ = store.Close()
	os.Exit(exitCode)
}

func TestTikvStore_Simple(t *testing.T) {
	// Set some key-value pair with different timestamp
	ctx := context.Background()
	key := Key("key")

	// Clean kv data
	err := store.Delete(ctx, key, math.MaxUint64)
	assert.Nil(t, err)

	// Ensure test data is not exist
	v, err := store.Get(ctx, key, math.MaxUint64)
	assert.Nil(t, v)
	assert.Nil(t, err)

	// Set value for key with multi-timestamp
	err = store.Set(ctx, key, Value("value_1"), 1)
	assert.Nil(t, err)
	err = store.Set(ctx, key, Value("value_2"), 2)
	assert.Nil(t, err)
	err = store.Set(ctx, key, Value("value_3"), 3)
	assert.Nil(t, err)

	// Get value for multi-timestamp
	v, err = store.Get(ctx, key, 1)
	assert.Nil(t, err)
	assert.Equal(t, Value("value_1"), v)

	v, err = store.Get(ctx, key, 2)
	assert.Nil(t, err)
	assert.Equal(t, Value("value_2"), v)

	v, err = store.Get(ctx, key, 3)
	assert.Nil(t, err)
	assert.Equal(t, Value("value_3"), v)

	v, err = store.Get(ctx, key, 4)
	assert.Nil(t, err)
	assert.Equal(t, Value("value_3"), v)

	v, err = store.Get(ctx, key, 0)
	assert.Nil(t, err)
	assert.Nil(t, v)

	// Delete test key
	err = store.Delete(ctx, key, 3)
	assert.Nil(t, err)

	// Ensure all test key is deleted
	v, err = store.Get(ctx, key, math.MaxUint64)
	assert.Nil(t, v)
	assert.Nil(t, err)
}

func TestTikvStore_PrefixKey(t *testing.T) {
	ctx := context.Background()
	key := Key("key")
	key1 := Key("key_1")

	// Clean kv data
	err := store.Delete(ctx, key, math.MaxUint64)
	assert.Nil(t, err)

	// Ensure test data is not exist
	v, err := store.Get(ctx, key, math.MaxUint64)
	assert.Nil(t, v)
	assert.Nil(t, err)
	v, err = store.Get(ctx, key1, math.MaxUint64)
	assert.Nil(t, v)
	assert.Nil(t, err)

	// Set some value for test key
	err = store.Set(ctx, key, Value("key_1"), 1)
	assert.Nil(t, err)
	err = store.Set(ctx, key1, Value("key1_1"), 1)
	assert.Nil(t, err)

	// Get Value
	v, err = store.Get(ctx, key, 1)
	assert.Nil(t, err)
	assert.Equal(t, Value("key_1"), v)

	v, err = store.Get(ctx, key1, 1)
	assert.Nil(t, err)
	assert.Equal(t, Value("key1_1"), v)

	// Delete key, value for "key" should nil
	err = store.Delete(ctx, key, 1)
	v, err = store.Get(ctx, key, 1)
	assert.Nil(t, v)
	assert.Nil(t, err)

	// Delete all test data
	err = store.Delete(ctx, key1, 1)
	assert.Nil(t, err)

}

func TestTikvStore_Batch(t *testing.T) {
	ctx := context.Background()

	// Prepare test data
	size := 0
	var testKeys []Key
	var testValues []Value
	for i := 0; size/conf.Raw.MaxBatchPutSize < 1; i++ {
		key := fmt.Sprint("key", i)
		size += len(key)
		testKeys = append(testKeys, []byte(key))
		value := fmt.Sprint("value", i)
		size += len(value)
		testValues = append(testValues, []byte(value))
		v, err := store.Get(ctx, Key(key), math.MaxUint64)
		assert.Nil(t, v)
		assert.Nil(t, err)
	}

	// Set kv data
	err := store.BatchSet(ctx, testKeys, testValues, 1)
	assert.Nil(t, err)

	// Get value
	checkValues, err := store.BatchGet(ctx, testKeys, 2)
	assert.NotNil(t, checkValues)
	assert.Nil(t, err)
	assert.Equal(t, len(checkValues), len(testValues))
	for i := range testKeys{
		assert.Equal(t, testValues[i], checkValues[i])
	}

	// Delete test data using multi go routine
	err = store.BatchDeleteMultiRoutine(ctx, testKeys, math.MaxUint64)
	assert.Nil(t, err)
	// Ensure all test key is deleted
	checkValues, err = store.BatchGet(ctx, testKeys, math.MaxUint64)
	assert.Nil(t, err)
	for _, value := range checkValues{
		assert.Nil(t, value)
	}

	// Delete test data
	err = store.BatchDelete(ctx, testKeys, math.MaxUint64)
	assert.Nil(t, err)
	// Ensure all test key is deleted
	checkValues, err = store.BatchGet(ctx, testKeys, math.MaxUint64)
	assert.Nil(t, err)
	for _, value := range checkValues{
		assert.Nil(t, value)
	}
}


