package coordinator

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"strings"
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
	client          *clientv3.Client
	logger          *slog.Logger
	keepAliveCancel context.CancelFunc
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
	requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	fallback := Assignment{Shard: stableShard(instanceID, totalShards), TotalShards: totalShards}
	lease, err := c.client.Grant(requestCtx, 30)
	if err != nil {
		c.logger.Warn("etcd lease failed, using local shard", slog.String("error", err.Error()))
		return fallback
	}

	key := fmt.Sprintf("/metro-pipeline/collectors/%s", instanceID)
	if _, err := c.client.Put(requestCtx, key, "active", clientv3.WithLease(lease.ID)); err != nil {
		c.logger.Warn("etcd assignment failed, using local shard", slog.String("error", err.Error()))
		return fallback
	}

	keepAliveCtx, keepAliveCancel := context.WithCancel(context.Background())
	c.keepAliveCancel = keepAliveCancel
	keepAlive, err := c.client.KeepAlive(keepAliveCtx, lease.ID)
	if err != nil {
		c.logger.Warn("etcd keepalive failed, using registered assignment", slog.String("error", err.Error()))
	} else {
		go drainKeepAlive(keepAlive)
	}

	response, err := c.client.Get(requestCtx, "/metro-pipeline/collectors/", clientv3.WithPrefix())
	if err != nil {
		c.logger.Warn("etcd collector listing failed, using local shard", slog.String("error", err.Error()))
		return fallback
	}
	instances := make([]string, 0, len(response.Kvs))
	for _, kv := range response.Kvs {
		instances = append(instances, strings.TrimPrefix(string(kv.Key), "/metro-pipeline/collectors/"))
	}
	sort.Strings(instances)
	for rank, activeInstanceID := range instances {
		if activeInstanceID == instanceID {
			return Assignment{Shard: rank % totalShards, TotalShards: totalShards}
		}
	}
	return fallback
}

func (c *EtcdCoordinator) Close() error {
	if c.keepAliveCancel != nil {
		c.keepAliveCancel()
	}
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

func drainKeepAlive(keepAlive <-chan *clientv3.LeaseKeepAliveResponse) {
	for range keepAlive {
	}
}
