package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/tundraws/laboratorywork14/src/go-collector/internal/aggregation"
	"github.com/tundraws/laboratorywork14/src/go-collector/internal/config"
	"github.com/tundraws/laboratorywork14/src/go-collector/internal/coordinator"
	"github.com/tundraws/laboratorywork14/src/go-collector/internal/simulator"
	"github.com/tundraws/laboratorywork14/src/go-collector/internal/transport"
	"github.com/tundraws/laboratorywork14/src/go-collector/internal/validation"
)

type App struct {
	cfg    config.Config
	logger *slog.Logger
}

func New(cfg config.Config, logger *slog.Logger) *App {
	return &App{cfg: cfg, logger: logger}
}

func (a *App) Run(ctx context.Context) error {
	coord := a.buildCoordinator()
	defer coord.Close()

	assignment := coord.Assign(ctx, a.cfg.InstanceID, a.cfg.TotalShards)
	sensors := simulator.FilterByShard(simulator.DefaultSensors(), assignment.Shard, assignment.TotalShards)
	a.logger.Info("collector shard assigned",
		slog.Int("shard", assignment.Shard),
		slog.Int("total_shards", assignment.TotalShards),
		slog.Int("sensors", len(sensors)),
	)

	publisher, err := transport.NewNATSPublisher(a.cfg.NATSURL, a.cfg.NATSSubject, a.logger)
	if err != nil {
		a.logger.Warn("nats publisher unavailable, collector will continue", slog.String("error", err.Error()))
	}
	defer publisher.Close()

	store := transport.NewAggregateStore(5000)
	server := transport.NewArrowServer(a.cfg.ArrowAddr, store, a.logger)
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	sim := simulator.New(sensors, a.cfg.EventsPerSecond, validation.PassengerEventValidator{}, time.Now().UnixNano())
	aggregator := aggregation.NewWindowAggregator(a.cfg.WindowDuration)
	aggregateBatches := aggregator.Aggregate(ctx, sim.Stream(ctx))

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return server.Shutdown(shutdownCtx)
		case err := <-serverErr:
			return err
		case batch, ok := <-aggregateBatches:
			if !ok {
				return nil
			}
			store.Add(batch)
			publisher.Publish(batch)
			a.logger.Info("aggregate batch produced", slog.Int("records", len(batch)))
		}
	}
}

func (a *App) buildCoordinator() coordinator.Coordinator {
	coord, err := coordinator.NewEtcdCoordinator(a.cfg.EtcdEndpoints, a.logger)
	if err != nil {
		a.logger.Warn("etcd unavailable, using static coordinator", slog.String("error", err.Error()))
		return coordinator.StaticCoordinator{}
	}
	return coord
}
