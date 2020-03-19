package db

import (
	"context"
	"encoding/base64"
	"time"

	"go.etcd.io/etcd/clientv3"
)

const DefaultEtcdEndpoint = "localhost:2379"

var KV KVStorage

type KVStorage interface {
	Get(string) (string, error)
	Set(string, string) error
	List(string) (map[string]string, error)
	Delete(string) (string, error)
}

type EtcdClient struct {
	client    *clientv3.Client
	endpoints string
	timeout   time.Duration
}

func InitKV() error {
	etcdCli, err := NewEtcdClient([]string{DefaultEtcdEndpoint})
	if err != nil {
		return err
	}
	KV = etcdCli
	return nil
}

func NewEtcdClient(endpoints []string) (*EtcdClient, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	etcdClient := &EtcdClient{
		client:  cli,
		timeout: time.Duration(5 * time.Second),
	}
	return etcdClient, nil
}

func (c *EtcdClient) Get(key string) (string, error) {
	ctx, _ := context.WithTimeout(context.Background(), c.timeout)
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if resp.Count < 1 {
		return "", nil
	}
	valueBytes, err := base64.RawStdEncoding.DecodeString(string(resp.Kvs[0].Value))
	if err != nil {
		return "", err
	}
	return string(valueBytes), nil
}

func (c *EtcdClient) Set(key string, value string) error {
	base64Value := base64.RawStdEncoding.EncodeToString([]byte(value))
	ctx, _ := context.WithTimeout(context.Background(), c.timeout)
	if _, err := c.client.Put(ctx, key, base64Value); err != nil {
		return err
	}
	return nil
}

func (c *EtcdClient) Delete(key string) (string, error) {
	ctx, _ := context.WithTimeout(context.Background(), c.timeout)
	resp, err := c.client.Delete(ctx, key, clientv3.WithPrevKV())
	if err != nil {
		return "", err
	}
	if len(resp.PrevKvs) < 1 {
		return "", nil
	}
	valueBytes, err := base64.RawStdEncoding.DecodeString(string(resp.PrevKvs[0].Value))
	if err != nil {
		return "", err
	}
	return string(valueBytes), nil
}

func (c *EtcdClient) List(prefix string) (map[string]string, error) {
	ctx, _ := context.WithTimeout(context.Background(), c.timeout)
	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	result := make(map[string]string)
	for _, kvs := range resp.Kvs {
		valueBytes, err := base64.RawStdEncoding.DecodeString(string(kvs.Value))
		if err != nil {
			return nil, err
		}
		result[string(kvs.Key)] = string(valueBytes)
	}
	return result, nil
}
