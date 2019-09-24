package client

import (
	"context"
	"github.com/subiz/goutils/log"
	"github.com/subiz/header"
	pb "github.com/subiz/header/kv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/naming"
	"time"
)

type KV struct {
	client header.KVClient
}

func dialGrpc(service string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	// Enabling WithBlock tells the client to not give up trying to find a server
	opts = append(opts, grpc.WithBlock())
	// However, we're still setting a timeout so that if the server takes too long, we still give up
	opts = append(opts, grpc.WithTimeout(10*time.Second))
	res, err := naming.NewDNSResolverWithFreq(5 * time.Second)
	if err != nil {
		return nil, err
	}
	opts = append(opts, grpc.WithBalancer(grpc.RoundRobin(res)))
	return grpc.Dial(service, opts...)
}

func dialKVService(service string) (header.KVClient, error) {
	conn, err := dialGrpc(service)
	if err != nil {
		log.Error(err, "unable to connect to kv service")
		return nil, err
	}
	return header.NewKVClient(conn), nil
}

func NewKV(service string) *KV {
	c, err := dialKVService(service)
	if err != nil {
		panic(err)
	}
	return &KV{client: c}
}

func (kv *KV) SetBytes(par, key string, data []byte, ttls ...int64) error {
	ttl := int64(0)
	if len(ttls) > 0 {
		ttl = ttls[0]
	}
	ctx := context.Background()
	_, err := kv.client.Set(ctx, &pb.Value{
		Partition: par,
		Key:       key,
		Bytes:     data,
		Ttl:       ttl,
	})
	return err
}

func (kv *KV) SetString(par, key, data string, ttls ...int64) error {
	ttl := int64(0)
	if len(ttls) > 0 {
		ttl = ttls[0]
	}
	ctx := context.Background()
	_, err := kv.client.Set(ctx, &pb.Value{
		Partition: par,
		Key:       key,
		Value:     data,
		Ttl:       ttl,
	})
	return err
}

func (kv *KV) GetBytes(par, key string) ([]byte, error) {
	ctx := context.Background()
	v, err := kv.client.Get(ctx, &pb.Key{Partition: par, Key: key})
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return v.GetBytes(), nil
}

func (kv *KV) GetString(par, key string) (string, error) {
	ctx := context.Background()
	v, err := kv.client.Get(ctx, &pb.Key{Partition: par, Key: key})
	if err != nil {
		return "", err
	}
	if v == nil {
		return "", nil
	}
	return v.GetValue(), nil
}

func (kv *KV) Has(par, key string) (bool, error) {
	ctx := context.Background()
	b, err := kv.client.Has(ctx, &pb.Key{Partition: par, Key: key})
	if err != nil {
		return false, err
	}
	if b == nil {
		return false, nil
	}
	return b.GetValue(), nil
}
