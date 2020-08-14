package main

import (
	"context"
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/pivotal-golang/bytefmt"
	"log"
	"math/rand"
	"os"
	minio "storage/internal/minio"
	tikv "storage/internal/tikv"
	"storage/pkg/types"
	"sync"
	"sync/atomic"
	"time"
)

// Global variables
var durationSecs, threads, loops, numVersion, batchOpSize int
var valueSize uint64
var valueData []byte
var batchValueData [][]byte
var setCount, getCount, deleteCount, setFailedCount, getFailedCount, deleteFailedCount, keyNum int32
var batchSetCount, batchGetCount, batchDeleteCount int32
var endTime, setFinish, getFinish, deleteFinish time.Time

var store types.Store
var wg sync.WaitGroup

func runSet() {
	for time.Now().Before(endTime) {
		num := atomic.AddInt32(&keyNum, 1)
		key := []byte(fmt.Sprint("key", num))
		for ver := 1; ver <= numVersion; ver++ {
			atomic.AddInt32(&setCount, 1)
			err := store.Set(context.Background(), key, valueData, uint64(ver))
			if err != nil {
				log.Fatalf("Error setting key %s, %s", key, err.Error())
				//atomic.AddInt32(&setCount, -1)
			}
		}
	}
	// Remember last done time
	setFinish = time.Now()
	wg.Done()
}

func runBatchSet() {
	for time.Now().Before(endTime) {
		num := atomic.AddInt32(&keyNum, int32(batchOpSize))
		keys := make([][]byte, batchOpSize)
		for n := batchOpSize; n > 0; n-- {
			keys[n-1] = []byte(fmt.Sprint("key", num-int32(n)))
		}
		for ver := 1; ver <= numVersion; ver++ {
			atomic.AddInt32(&batchSetCount, 1)
			err := store.BatchSet(context.Background(), keys, batchValueData, uint64(numVersion))
			if err != nil {
				log.Fatalf("Error setting batch keys %s %s", keys, err.Error())
				//atomic.AddInt32(&batchSetCount, -1)
			}
		}
	}
	setFinish = time.Now()
	wg.Done()
}

func runGet() {
	for time.Now().Before(endTime) {
		atomic.AddInt32(&getCount, 1)
		num := atomic.AddInt32(&keyNum, 1)
		key := []byte(fmt.Sprint("key", num))
		_, err := store.Get(context.Background(), key, uint64(numVersion))
		if err != nil {
			log.Fatalf("Error getting key %s, %s", key, err.Error())
			//atomic.AddInt32(&getCount, -1)
		}
	}
	// Remember last done time
	getFinish = time.Now()
	wg.Done()
}

func runBatchGet() {
	for time.Now().Before(endTime) {
		num := atomic.AddInt32(&keyNum, int32(batchOpSize))
		keys := make([][]byte, batchOpSize)
		for n := batchOpSize; n > 0; n-- {
			keys[n-1] = []byte(fmt.Sprint("key", num-int32(n)))
		}
		atomic.AddInt32(&batchGetCount, 1)
		_, err := store.BatchGet(context.Background(), keys, uint64(numVersion))
		if err != nil {
			log.Fatalf("Error getting key %s, %s", keys, err.Error())
			//atomic.AddInt32(&batchGetCount, -1)
		}
	}
	// Remember last done time
	getFinish = time.Now()
	wg.Done()
}

func runDelete() {
	for time.Now().Before(endTime) {
		atomic.AddInt32(&deleteCount, 1)
		num := atomic.AddInt32(&keyNum, 1)
		key := []byte(fmt.Sprint("key", num))
		err := store.Delete(context.Background(), key, uint64(numVersion))
		if err != nil {
			log.Fatalf("Error getting key %s, %s", key, err.Error())
			//atomic.AddInt32(&deleteCount, -1)
		}
	}
	// Remember last done time
	deleteFinish = time.Now()
	wg.Done()
}

func runBatchDelete() {
	for time.Now().Before(endTime) {
		num := atomic.AddInt32(&keyNum, int32(batchOpSize))
		keys := make([][]byte, batchOpSize)
		for n := batchOpSize; n > 0; n-- {
			keys[n-1] = []byte(fmt.Sprint("key", num-int32(n)))
		}
		atomic.AddInt32(&batchDeleteCount, 1)
		err := store.BatchDelete(context.Background(), keys, uint64(numVersion))
		if err != nil {
			log.Fatalf("Error getting key %s, %s", keys, err.Error())
			//atomic.AddInt32(&batchDeleteCount, -1)
		}
	}
	// Remember last done time
	getFinish = time.Now()
	wg.Done()
}

func main() {
	// Parse command line
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.IntVar(&durationSecs, "d", 5, "Duration of each test in seconds")
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.IntVar(&loops, "l", 1, "Number of times to repeat test")
	var sizeArg string
	var storeType string
	myflag.StringVar(&sizeArg, "z", "0.1K", "Size of objects in bytes with postfix K, M, and G")
	myflag.StringVar(&storeType, "s", "tikv", "Storage type, tikv or minio")
	myflag.IntVar(&numVersion, "v", 1, "Max versions for each key")
	myflag.IntVar(&batchOpSize, "b", 10, "Batch operation kv pair number")

	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	// Check the arguments
	var err error
	if valueSize, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}
	switch storeType {
	case "tikv":
		//var (
		//	pdAddr = []string{"127.0.0.1:2379"}
		//	conf   = config.Default()
		//)
		store, err = tikv.NewTikvStore(context.Background())
		if err != nil {
			panic("Error when creating storage " + err.Error())
		}
	case "minio":
		store, err = minio.NewMinIOStore(context.Background())
		if err != nil {
			panic("Error when creating storage " + err.Error())
		}
	default:
		panic("Not supported storage type")
	}

	// Echo the parameters
	fmt.Printf("Parameters: duration=%d, threads=%d, loops=%d, size=%s, versions=%d\n", durationSecs, threads, loops, sizeArg, numVersion)

	// Init test data
	valueData = make([]byte, valueSize)
	rand.Read(valueData)
	hasher := md5.New()
	hasher.Write(valueData)

	batchValueData = make([][]byte, batchOpSize)
	for i := range batchValueData {
		batchValueData[i] = make([]byte, valueSize)
		rand.Read(batchValueData[i])
		hasher := md5.New()
		hasher.Write(batchValueData[i])
	}

	// Loop running the tests
	for loop := 1; loop <= loops; loop++ {

		// reset counters
		setCount = 0
		setFailedCount = 0
		getCount = 0
		getFailedCount = 0
		deleteCount = 0
		deleteFailedCount = 0
		keyNum = 0

		// Run the set case
		startTime := time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runSet()
		}
		wg.Wait()

		setTime := setFinish.Sub(startTime).Seconds()

		bps := float64(uint64(setCount)*valueSize) / setTime
		fmt.Printf("Loop %d: PUT time %.1f secs, kv pairs = %d, speed = %sB/sec, %.1f operations/sec. Failes = %d \n",
			loop, setTime, setCount, bytefmt.ByteSize(uint64(bps)), float64(setCount)/setTime, setFailedCount)

		// Run the get case
		startTime = time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runGet()
		}
		wg.Wait()

		getTime := getFinish.Sub(startTime).Seconds()
		bps = float64(uint64(getCount)*valueSize) / getTime
		fmt.Printf("Loop %d: GET time %.1f secs, kv pairs = %d, speed = %sB/sec, %.1f operations/sec. Failes = %d \n",
			loop, getTime, getCount, bytefmt.ByteSize(uint64(bps)), float64(getCount)/getTime, getFailedCount)

		// Run the delete case
		startTime = time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runDelete()
		}
		wg.Wait()

		deleteTime := deleteFinish.Sub(startTime).Seconds()
		bps = float64(uint64(deleteCount)*valueSize) / deleteTime
		fmt.Printf("Loop %d: Delete time %.1f secs, kv pairs = %d, %.1f operations/sec. Failes = %d \n",
			loop, deleteTime, deleteCount, float64(deleteCount)/deleteTime, deleteFailedCount)

		// Run the batchSet case
		startTime = time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runBatchSet()
		}
		wg.Wait()

		setTime = setFinish.Sub(startTime).Seconds()
		bps = float64(uint64(batchSetCount)*valueSize*uint64(batchOpSize)) / setTime
		fmt.Printf("Loop %d: BATCH PUT time %.1f secs, batchs = %d, kv pairs = %d, speed = %sB/sec, %.1f operations/sec. Failes = %d \n",
			loop, setTime, batchSetCount, batchSetCount*int32(batchOpSize), bytefmt.ByteSize(uint64(bps)), float64(batchSetCount)/setTime, setFailedCount)

		// Run the batchGet case
		startTime = time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runBatchGet()
		}
		wg.Wait()

		getTime = getFinish.Sub(startTime).Seconds()
		bps = float64(uint64(batchGetCount)*valueSize*uint64(batchOpSize)) / getTime
		fmt.Printf("Loop %d: BATCH GET time %.1f secs, batchs = %d, kv pairs = %d, speed = %sB/sec, %.1f operations/sec. Failes = %d \n",
			loop, setTime, batchGetCount, batchGetCount*int32(batchOpSize), bytefmt.ByteSize(uint64(bps)), float64(batchGetCount)/setTime, setFailedCount)

		// Run the batchDelete case
		startTime = time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runBatchDelete()
		}
		wg.Wait()

		deleteTime = setFinish.Sub(startTime).Seconds()
		bps = float64(uint64(batchDeleteCount)*valueSize*uint64(batchOpSize)) / setTime
		fmt.Printf("Loop %d: BATCH DELETE time %.1f secs, batchs = %d, kv pairs = %d, %.1f operations/sec. Failes = %d \n",
			loop, setTime, batchDeleteCount, batchDeleteCount*int32(batchOpSize), float64(batchDeleteCount)/setTime, setFailedCount)
	}
}
