package coordinator

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Assignment struct {
	Shard       int
	TotalShards int
}

type Coordinator interface {
	Assign(ctx context.Context, instanceID string, totalShards int) Assignment
	Close() error
}

type EtcdCoordinator struct {
	client *clientv3.Client
	logger *slog.Logger
}

type StaticCoordinator struct{}

func NewEtcdCoordinator(endpoints []string, logger *slog.Logger) (*EtcdCoordinator, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &EtcdCoordinator{client: client, logger: logger}, nil
}

func (c *EtcdCoordinator) Assign(ctx context.Context, instanceID string, totalShards int) Assignment {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	shard := stableShard(instanceID, totalShards)
	key := fmt.Sprintf("/metro-pipeline/collectors/%s", instanceID)
	if _, err := c.client.Put(ctx, key, fmt.Sprintf("%d", shard)); err != nil {
		c.logger.Warn("etcd assignment failed, using local shard", slog.String("error", err.Error()))
	}
	return Assignment{Shard: shard, TotalShards: totalShards}
}

func (c *EtcdCoordinator) Close() error {
	return c.client.Close()
}

func (StaticCoordinator) Assign(_ context.Context, instanceID string, totalShards int) Assignment {
	return Assignment{Shard: stableShard(instanceID, totalShards), TotalShards: totalShards}
}

func (StaticCoordinator) Close() error {
	return nil
}

func stableShard(instanceID string, totalShards int) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(instanceID))
	return int(hasher.Sum32() % uint32(totalShards))
}
