package minio_driver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	. "storage/pkg/types"
	"storage/internal/minio/codec"
)

//TODO: How to define bucketName?
var bucketName = "zcbucket"

type minioStore struct {
	client *minio.Client
}


func NewMinIOStore(ctx context.Context) (*minioStore, error) {
	// to-do read conf
	var endPoint = "127.0.0.1:9000"
	var accessKeyID = "testminio"
	var secretAccessKey = "testminio"
	var useSSL = false

	minioClient, err := minio.New(endPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		return nil, err
	}

	bucketExists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	if !bucketExists {
		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, err
		}
	}
	return &minioStore{
		client: minioClient,
	}, nil
}

func (s *minioStore) Get(ctx context.Context, key Key, timestamp uint64) (Value, error) {

	for objectInfo := range s.listObjectsKeys(ctx, key, timestamp){
		if objectInfo.Err != nil {
			fmt.Println(objectInfo.Err)
			return nil, objectInfo.Err
		}
		object, err := s.client.GetObject(ctx, bucketName, objectInfo.Key, minio.GetObjectOptions{})

		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		buf := make([]byte, objectInfo.Size)
		n, err := object.Read(buf)
		if err != nil && err != io.EOF {
			fmt.Println(err)
			return nil, err
		}
		return buf[:n], nil
	}

	return nil, nil
}

func (s *minioStore) BatchGet(ctx context.Context, keys []Key, timestamp uint64) ([]Value, error) {

	errCh := make(chan error)
	valueLenth := len(keys)
	values := make([]Value, valueLenth)
	f := func(ctx context.Context, keys []Key, values []Value, timestamp uint64) {
		for i := 0; i < len(keys); i++ {
			value, err := s.Get(ctx, keys[i], timestamp)
			values[i] = value
			errCh <- err
		}
	}

	batchSize, batchNums := optimalArgs(len(keys))

	for i := 0; i < batchNums; i++ {
		j := i
		go func() {
			start, end := j*batchSize, (j+1)*batchSize
			if len(keys) < end {
				end = len(keys)
			}
			f(ctx, keys[start:end], values, timestamp)
		}()
	}

	for i := 0; i < len(keys); i++ {
		if err := <- errCh; err != nil {
			return values, err
		}
	}


	return values, nil
}

func (s *minioStore) Set(ctx context.Context, key Key, v Value, timestamp uint64) error {
	minioKey := codec.MvccEncode(key, timestamp)

	reader := bytes.NewReader(v)
	_, err := s.client.PutObject(ctx, bucketName, minioKey, reader, int64(len(v)), minio.PutObjectOptions{})

	if err != nil {
		return err
	}

	return err
}

func (s *minioStore) BatchSet(ctx context.Context, keys []Key, values []Value, timestamp uint64) error {

	errCh := make(chan error)
	f := func(ctx context.Context, keys []Key, values []Value, timestamp uint64) {
		for i := 0; i < len(keys); i++ {
			errCh <- s.Set(ctx, keys[i], values[i], timestamp)
		}
	}

	batchSize, batchNums := optimalArgs(len(keys))

	for i := 0; i < batchNums; i++ {
		j := i
		go func() {
			start, end := j*batchSize, (j+1)*batchSize
			if len(keys) < end {
				end = len(keys)
			}
			f(ctx, keys[start:end], values[start:end], timestamp)
		}()
	}

	for i := 0; i < len(keys); i++ {
		if err := <- errCh; err != nil {
			return err
		}
	}

	return nil
}

func (s *minioStore) Delete(ctx context.Context, key Key, timestamp uint64) error {
	objectsCh := make(chan minio.ObjectInfo)

	go func() {
		defer close(objectsCh)
		for object := range s.listObjectsKeys(ctx, key, timestamp) {
			objectsCh <- object
		}
	}()

	opts := minio.RemoveObjectsOptions{
		GovernanceBypass: true,
	}

	for rErr := range s.client.RemoveObjects(ctx, bucketName, objectsCh, opts) {
		if rErr.Err != nil {
			return rErr.Err
		}
	}
	return nil
}

func (s *minioStore) BatchDelete(ctx context.Context, keys []Key, timestamp uint64) error {

	errCh := make(chan error)
	f := func(ctx context.Context, keys []Key, timestamp uint64) {
		for i := 0; i < len(keys); i++ {
			errCh <- s.Delete(ctx, keys[i], timestamp)
		}
	}

	batchSize, batchNums := optimalArgs(len(keys))
	for i := 0; i < batchNums; i++ {
		j := i
		go func() {
			start, end := j*batchSize, (j+1)*batchSize
			if len(keys) < end {
				end = len(keys)
			}
			f(ctx, keys[start:end], timestamp)
		}()
	}

	for i := 0; i < len(keys); i++ {
		if err := <- errCh; err != nil {
			return err
		}
	}

	return nil
}

func (s *minioStore) listObjectsKeys(ctx context.Context, key Key, timestamp uint64) <-chan minio.ObjectInfo {
	minioKey := codec.MvccEncode(key, timestamp)

	objectsCh := make(chan minio.ObjectInfo)

	go func() {
		defer close(objectsCh)
		for object := range s.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: string(key)}){
			if object.Err != nil {
				fmt.Println(object.Err)
				objectsCh <- object
			}
			splitKey := strings.Split(object.Key, "_")
			if string(key) == strings.Join(splitKey[:len(splitKey)-1], "_") {
				if minioKey <= object.Key {
					objectsCh <- object
				}
			}
		}
	}()

	return objectsCh
}

func (s *minioStore) Close() error{
	return nil
}

func optimalArgs(keysLength int) (batchSize int, batchNums int) {
	maxThread := 500
	batchSize = 1

	if keysLength / batchSize > maxThread {
		batchSize = keysLength / maxThread
	}

	batchNums = keysLength / batchSize

	if keysLength % batchSize != 0 {
		batchNums = keysLength / batchSize + 1
	}

	return batchSize, batchNums
}